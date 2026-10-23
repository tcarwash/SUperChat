"""Bound background chatter while preserving automatic and signature repair."""
import tempfile
import time
import unittest
from unittest.mock import patch

from kisschat.app import Chat
from kisschat.protocol import (MAX_FRAME, MAX_SPARSE_SEQUENCE_REQUESTS,
                               message_parts, pack, unpack)
from scripts.simulate_mesh import make_args


class RecordingRadio:
    port = 0
    manual_mode = None
    mode_catalog = ()

    def __init__(self):
        self.sent = []

    def send(self, payload, *, priority=0):
        self.sent.append((payload, priority))

    def close(self):
        pass


class SyncAirtimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.chat = Chat(make_args('A', 'Alice', self.temp.name + '/a.sqlite'))
        self.chat.data = RecordingRadio()
        self.addCleanup(self.chat.close)
        self.peer, self.origin = '02' * 16, '03' * 16

    def hear(self, record):
        self.chat._on_frame(0, pack(record))

    def records(self):
        return [unpack(payload) for payload, _ in self.chat.data.sent]

    def inventory(self, **fields):
        return dict(t='i', p=self.peer, n='K0BBB', o=self.origin,
                    a=1, m=1, ttl=4, **fields)

    def test_neighbor_query_is_compact_and_does_not_echo(self):
        self.hear(self.inventory(sig=0))
        self.assertEqual(len(self.records()), 1)
        request = self.records()[0]
        self.assertEqual((request['t'], request['ttl']), ('q', 0))
        self.assertLess(len(self.chat.data.sent[0][0]), 90)
        self.assertEqual(request['r'], [1])
        self.assertNotIn('h', request)
        self.assertEqual(self.chat.data.sent[0][0][3:4], b'S')

    def test_matching_unsigned_inventory_and_summary_are_quiet(self):
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.hear(self.inventory(sig=0))
        self.hear(dict(t='k', **{'from': self.peer}, h=[[self.origin, 1, 1]]))
        self.assertEqual(self.records(), [])

    def test_signature_advertisement_does_not_request_proof_only_pages(self):
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.hear(self.inventory(sig=1))
        self.assertEqual(self.records(), [])

    def test_inventory_without_proof_availability_does_not_probe_proofs(self):
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.hear(self.inventory())
        self.assertEqual(self.records(), [])

    def test_completed_proofs_remain_quiet_on_inventory(self):
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.chat.store.set_auth(self.origin, 1, b'proof', 'pending')
        self.hear(self.inventory(sig=1))
        self.assertEqual(self.records(), [])

    def test_single_hop_response_keeps_body_and_end_in_order(self):
        row = self.chat.store.insert_local('#general', 'Alice', int(time.time()), 'hello')
        self.hear(dict(t='q', to=self.chat.store.node, **{'from': self.peer},
                       o=row['o'], a=0, b=1, ttl=0))
        self.assertEqual([r['t'] for r in self.records()], ['d', 'e'])
        self.assertEqual(self.records()[-1]['ttl'], 0)

    def test_timeout_asks_for_manifest_without_losing_flag_in_binary_codec(self):
        self.hear(self.inventory(sig=0))
        # The data arrived, but its addressed page end did not.
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=time.monotonic() + 20):
            self.chat._retry_pending_sync_requests()
        self.assertEqual(len(self.records()), 1)
        self.assertTrue(self.records()[0]['j'])
        self.assertEqual(self.records()[0]['k'], 1)

    def test_lost_query_retries_data_instead_of_requesting_only_a_manifest(self):
        self.hear(self.inventory(sig=0))
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=time.monotonic() + 20):
            self.chat._retry_pending_sync_requests()
        request = self.records()[0]
        self.assertFalse(request.get('j'))
        self.assertEqual(request['k'], 1)
        self.assertLess(len(self.chat.data.sent[0][0]), 90)

    def test_page_timeout_waits_for_silence_instead_of_retrying_active_transfer(self):
        now = time.monotonic()
        with patch('kisschat.app.secrets.randbelow', return_value=0), \
                patch('kisschat.app.time.monotonic', return_value=now):
            self.hear(self.inventory(sig=0))
        self.chat.data.sent.clear()
        with patch('kisschat.app.secrets.randbelow', return_value=0), \
                patch('kisschat.app.time.monotonic', return_value=now + 10):
            self.chat._note_sync_progress(self.origin, 1)
        with patch('kisschat.app.time.monotonic', return_value=now + 13):
            self.chat._retry_pending_sync_requests()
        self.assertEqual(self.records(), [])
        self.assertEqual(next(iter(self.chat.pending_sync_requests.values()))[2], now + 22)

    def test_periodic_sync_advertises_without_requesting_neighbor_bursts(self):
        self.chat.store.insert_local('#general', 'Alice', int(time.time()), 'hello')
        self.chat.sync(discover=False)
        self.assertEqual([r['t'] for r in self.records()], ['i'])
        self.assertEqual(self.records()[0]['sig'], 0)

    def test_publish_advertises_highwater_and_serves_only_missing_messages(self):
        timestamp = int(time.time())
        for seq in range(1, 16):
            self.chat.store.insert(self.origin, seq, '#general', 'Bob',
                                   timestamp, f'history {seq}')

        receiver = Chat(make_args('B', 'Bob', self.temp.name + '/b.sqlite'))
        receiver.data = RecordingRadio()
        self.addCleanup(receiver.close)
        for seq in range(1, 11):
            receiver.store.insert(self.origin, seq, '#general', 'Bob',
                                  timestamp, f'history {seq}')

        with patch('kisschat.app.secrets.randbelow', return_value=0):
            self.chat.publish('new message')
        published = self.records()
        self.assertEqual([r['t'] for r in published], ['d', 'k'])
        self.assertIn([self.origin, 1, 15], published[1]['h'])
        self.assertIn([self.chat.store.node, 1, 1], published[1]['h'])

        receiver._on_frame(0, self.chat.data.sent[0][0])
        receiver._on_frame(0, self.chat.data.sent[1][0])
        requests = [unpack(payload) for payload, _ in receiver.data.sent]
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['o'], self.origin)
        self.assertEqual((requests[0]['a'], requests[0]['b']), (10, 15))

        self.chat.data.sent.clear()
        self.chat._on_frame(0, receiver.data.sent[0][0])
        response = self.records()
        message_response = [r for r in response if r['t'] == 'd']
        self.assertEqual([r['s'] for r in message_response], list(range(11, 16)))
        self.assertTrue(all(r['o'] == self.origin for r in message_response))
        self.assertTrue(all(r['from'] == self.chat.store.node for r in message_response))
        self.assertEqual(response[-1]['t'], 'e')
        for payload, _ in self.chat.data.sent:
            record = unpack(payload)
            if record and record.get('t') == 'd':
                receiver._on_frame(0, payload)
        self.assertEqual(receiver.store.reception_for(self.origin, 11), 'relayed')

    def test_overheard_out_of_order_message_is_skipped_in_one_sparse_binary_sync(self):
        timestamp = int(time.time())
        for seq in range(1, 16):
            self.chat.store.insert(self.origin, seq, '#general', 'Bob',
                                   timestamp, f'history {seq}')
        listener = Chat(make_args('C', 'Carol', self.temp.name + '/c.sqlite'))
        listener.data = RecordingRadio()
        self.addCleanup(listener.close)

        heard = self.chat.store.messages_between(self.origin, 9, 10, limit=1)[0]
        for frame in message_parts(origin=heard['o'], seq=heard['s'], channel=heard['c'],
                                   nick=heard['n'], timestamp=heard['ts'], text=heard['b'],
                                   sender=self.chat._message_sender_id()):
            listener._on_frame(0, frame)
        self.assertTrue(listener.store.has_message(self.origin, 10))
        self.assertEqual(listener.store.contiguous(self.origin), 0)

        inventory = {'t': 'k', 'from': self.chat.store.node,
                     'h': [[self.origin, 1, 15]]}
        listener._on_frame(0, pack(inventory))
        self.assertEqual(len(listener.data.sent), 1)
        request_wire = listener.data.sent[0][0]
        request = unpack(request_wire)
        expected_missing = [*range(1, 10), *range(11, 16)]
        self.assertEqual(request['r'], expected_missing)
        self.assertNotIn('h', request)
        self.assertEqual(request_wire[3:4], b'S')

        self.chat.data.sent.clear()
        self.chat._on_frame(0, request_wire)
        response = [unpack(payload) for payload, _ in self.chat.data.sent]
        data_records = [record for record in response if record and record.get('t') == 'd']
        end_records = [record for record in response if record and record.get('t') == 'e']
        self.assertEqual({record['s'] for record in data_records}, set(expected_missing))
        self.assertEqual(len(data_records), len(expected_missing))
        self.assertTrue(all(not record.get('to') for record in data_records))
        self.assertEqual(len(end_records), 1)
        for payload, _ in self.chat.data.sent:
            record = unpack(payload)
            if record and record.get('t') in {'d', 'e'}:
                listener._on_frame(0, payload)
        self.assertTrue(all(listener.store.has_message(self.origin, seq)
                            for seq in range(1, 16)))
        self.assertEqual(listener.store.contiguous(self.origin), 15)

        listener.data.sent.clear()
        listener._on_frame(0, pack(inventory))
        self.assertEqual(listener.data.sent, [])

    def test_sparse_binary_sync_caps_each_exchange_at_36_and_skips_heard_rows(self):
        timestamp = int(time.time())
        for seq in range(1, 51):
            self.chat.store.insert(self.origin, seq, '#general', 'Bob',
                                   timestamp, f'history {seq}')
        listener = Chat(make_args('C', 'Carol', self.temp.name + '/large-c.sqlite'))
        listener.data = RecordingRadio()
        self.addCleanup(listener.close)
        heard = self.chat.store.messages_between(self.origin, 24, 25, limit=1)[0]
        for frame in message_parts(origin=heard['o'], seq=heard['s'], channel=heard['c'],
                                   nick=heard['n'], timestamp=heard['ts'], text=heard['b'],
                                   sender=self.chat._message_sender_id()):
            listener._on_frame(0, frame)

        listener._on_frame(0, pack({'t': 'k', 'from': self.chat.store.node,
                                    'h': [[self.origin, 1, 50]]}))
        requests = [payload for payload, _ in listener.data.sent
                    if (record := unpack(payload)) and record.get('t') == 'q']
        self.assertEqual(len(requests), 1)
        first_request = unpack(requests[0])
        self.assertFalse(first_request['x'])
        self.assertEqual(len(first_request['r']), MAX_SPARSE_SEQUENCE_REQUESTS)
        self.assertEqual(requests[0][3:4], b'S')
        self.assertEqual(len(requests[0]), MAX_FRAME - 1)

        def serve(request_wire):
            self.chat.data.sent.clear()
            self.chat._on_frame(0, request_wire)
            response_frames = list(self.chat.data.sent)
            response = [unpack(payload) for payload, _ in response_frames]
            data_records = [item for item in response if item and item.get('t') == 'd']
            end_records = [item for item in response if item and item.get('t') == 'e']
            self.assertEqual(len(end_records), 1)
            listener.data.sent.clear()
            for (payload, _), item in zip(response_frames, response):
                if item and item.get('t') in {'d', 'e'}:
                    listener._on_frame(0, payload)
            return [item['s'] for item in data_records]

        first_sequences = serve(requests[0])
        self.assertEqual(len(first_sequences), 36)
        self.assertEqual(set(first_sequences), set(range(1, 38)) - {25})
        self.assertEqual(listener.store.contiguous(self.origin), 37)
        self.assertFalse(any(listener.store.has_message(self.origin, seq)
                             for seq in range(38, 51)))

        listener.sync(retry=True, discover=False)
        listener.data.sent.clear()
        listener._on_frame(0, pack({'t': 'k', 'from': self.chat.store.node,
                                    'h': [[self.origin, 1, 50]]}))
        next_requests = [payload for payload, _ in listener.data.sent
                         if (record := unpack(payload)) and record.get('t') == 'q']
        self.assertEqual(len(next_requests), 1)
        next_request = unpack(next_requests[0])
        self.assertEqual(next_request['r'], list(range(38, 51)))
        self.assertFalse(next_request['x'])
        second_sequences = serve(next_requests[0])
        self.assertEqual(second_sequences, list(range(38, 51)))
        self.assertTrue(all(listener.store.has_message(self.origin, seq)
                            for seq in range(1, 51)))
        self.assertEqual(listener.store.contiguous(self.origin), 50)

    def test_history_sync_settings_persist_for_the_database(self):
        args = make_args('B', 'Bob', self.temp.name + '/settings.sqlite')
        args.sync_history_hours = 4
        args.sync_history_messages = 25
        configured = Chat(args)
        configured.data = RecordingRadio()
        self.addCleanup(configured.close)
        self.assertEqual(configured.sync_history_hours, 4)
        self.assertEqual(configured.sync_history_messages, 25)

        reopened = Chat(make_args('B', 'Bob', self.temp.name + '/settings.sqlite'))
        reopened.data = RecordingRadio()
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.sync_history_hours, 4)
        self.assertEqual(reopened.sync_history_messages, 25)

    def test_history_message_budget_is_global_across_origins(self):
        args = make_args('B', 'Bob', self.temp.name + '/limited.sqlite')
        args.sync_history_messages = 3
        receiver = Chat(args)
        receiver.data = RecordingRadio()
        self.addCleanup(receiver.close)
        other_origin = '04' * 16
        timestamp = int(time.time())
        for origin in (self.origin, other_origin):
            for seq in range(1, 11):
                self.chat.store.insert(origin, seq, '#general', 'Alice', timestamp,
                                       f'{origin[:2]} message {seq}')
        receiver._on_frame(0, pack({
            't': 'k', 'from': self.chat.store.node,
            'h': [[self.origin, 1, 10], [other_origin, 1, 10]],
        }))

        requests = [unpack(payload) for payload, _ in receiver.data.sent]
        self.assertEqual(len(requests), 2)
        self.assertEqual([request['b'] - request['a'] for request in requests], [1, 2])
        self.assertEqual(sum(request['b'] - request['a'] for request in requests), 3)
        self.chat.data.sent.clear()
        for payload, _ in receiver.data.sent:
            self.chat._on_frame(0, payload)
        sent_messages = [unpack(payload) for payload, _ in self.chat.data.sent
                         if unpack(payload)['t'] == 'd']
        self.assertEqual(len(sent_messages), 3)

        receiver.data.sent.clear()
        receiver.sync(retry=True, discover=False)
        next_origin = '05' * 16
        receiver._on_frame(0, pack({
            't': 'k', 'from': self.peer,
            'h': [[next_origin, 1, 10]],
        }))
        next_requests = [unpack(payload) for payload, _ in receiver.data.sent]
        self.assertEqual(len(next_requests), 1)
        self.assertEqual((next_requests[0]['a'], next_requests[0]['b']), (0, 3))

    def test_responder_applies_configured_history_age_cutoff(self):
        args = make_args('B', 'Bob', self.temp.name + '/cutoff.sqlite')
        args.sync_history_hours = 4
        responder = Chat(args)
        responder.data = RecordingRadio()
        self.addCleanup(responder.close)
        now = int(time.time())
        responder.store.insert(self.origin, 1, '#general', 'Bob', now - 6 * 3600,
                               'outside cutoff')
        responder.store.insert(self.origin, 2, '#general', 'Bob', now - 3 * 3600,
                               'inside cutoff')
        responder._on_frame(0, pack({
            't': 'q', 'to': responder.store.node, 'from': self.peer,
            'o': self.origin, 'a': 0, 'b': 2, 'd': now - 48 * 3600,
            'ttl': 0,
        }))

        response = [unpack(payload) for payload, _ in responder.data.sent]
        message_rows = [record for record in response if record['t'] == 'd']
        self.assertEqual([record['s'] for record in message_rows], [2])

    def test_signature_advertisement_does_not_retry_after_message_window_completed(self):
        self.chat.store.insert(self.origin, 1, '#general', 'Bob', int(time.time()), 'hello')
        self.chat.completed_sync_windows[(self.origin, 0, 1)] = time.monotonic() + 300
        self.hear(self.inventory(sig=0))
        self.assertEqual(self.records(), [])
        self.hear(self.inventory(sig=1))
        self.assertEqual(self.records(), [])

    def test_nodes_do_not_repeat_the_same_collision_retry_deadline(self):
        now = time.monotonic()
        with patch('kisschat.app.secrets.randbelow', side_effect=[0, 1000]), \
                patch('kisschat.app.time.monotonic', return_value=now):
            self.hear(self.inventory(sig=0))
            other = self.inventory(sig=0)
            other['o'] = '04' * 16
            self.hear(other)
        deadlines = sorted(item[2] for item in self.chat.pending_sync_requests.values())
        self.assertEqual(deadlines, [now + 12, now + 18])

    def test_new_inventory_does_not_delete_another_pages_due_retry(self):
        now = time.monotonic()
        with patch('kisschat.app.time.monotonic', return_value=now):
            self.hear(self.inventory(sig=0))
        with patch('kisschat.app.time.monotonic', return_value=now + 20):
            other = self.inventory(sig=0)
            other['o'] = '04' * 16
            self.hear(other)
            self.assertEqual(len(self.chat.pending_sync_requests), 2)
            self.chat._retry_pending_sync_requests()
        retries = [r for r in self.records() if r.get('o') == self.origin and r.get('k') == 1]
        self.assertEqual(len(retries), 1)

    def test_routine_sync_preserves_active_requests_when_their_retry_is_due(self):
        now = time.monotonic()
        with patch('kisschat.app.time.monotonic', return_value=now):
            self.hear(self.inventory(sig=0))
        with patch('kisschat.app.time.monotonic', return_value=now + 20):
            self.chat.sync(discover=False)
            self.chat._retry_pending_sync_requests()
        self.assertEqual(len(self.chat.pending_sync_requests), 1)
        self.assertTrue(any(r.get('k') == 1 for r in self.records()))


class TransportDebugTests(unittest.TestCase):
    def test_trace_separates_queue_acceptance_from_socket_submission(self):
        from kisschat.data2g import Data2G
        trace = []
        radio = Data2G('localhost', 'K0AAA', transport='ax25',
                       on_debug=lambda event, value: trace.append((event, value)))

        class Socket:
            def sendall(self, _wire):
                radio.stop_event.set()

        radio.kiss_socket = Socket()
        radio.connected_event.set()
        record = dict(t='w', p='01' * 16, v=2, ttl=0)
        radio.send(pack(record))
        self.assertEqual([event for event, _ in trace], ['TX_QUEUE'])
        radio._write_kiss()
        self.assertEqual([event for event, _ in trace], ['TX_QUEUE', 'TX_SUBMIT'])
        self.assertEqual(trace[1][1]['records'], [record])
        self.assertEqual(trace[1][1]['queued'], 0)


if __name__ == '__main__':
    unittest.main()
