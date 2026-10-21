"""Images stay public while one requester controls bounded transfer windows."""
from io import BytesIO
import hashlib
import random
import tempfile
import time
import unittest
from unittest.mock import patch

from PIL import Image

from kisschat.app import Chat, IMAGE_RETRY_SECONDS, IMAGE_RESPONSE_SECONDS, IMAGE_REQUEST_WINDOW
from kisschat.protocol import image_parts, image_part, pack, unpack
from scripts.simulate_mesh import make_args


class Radio:
    port = 0

    def __init__(self):
        self.sent = []

    def send(self, payload, *, priority=0):
        self.sent.append(unpack(payload))

    def close(self):
        pass


class ImageWindowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.chat = Chat(make_args('C', 'Casey', self.temp.name + '/c.sqlite'))
        self.addCleanup(self.chat.close)
        self.chat.data = Radio()
        image = Image.frombytes('RGB', (160, 120), random.Random(7).randbytes(160 * 120 * 3))
        buffer = BytesIO()
        image.save(buffer, 'JPEG', quality=35, optimize=True)
        self.body = buffer.getvalue()
        self.digest = hashlib.sha256(self.body).hexdigest()
        self.origin = '03' * 16
        self.chat.store.insert(self.origin, 1, '#general', 'Alice', int(time.time()), 'image')
        self.assertTrue(self.chat.store.set_attachment(self.origin, 1, self.digest,
                                                      len(self.body), 160, 120))
        self.attachment = self.chat.store.attachment_for(self.origin, 1)
        self.key = (self.origin, 1, self.digest)

    def frames(self, recipient=None, indexes=None, request_id='12345678'):
        return image_parts(recipient=recipient or self.chat.store.node,
                           origin=self.origin, seq=1, digest=self.digest,
                           data=self.body, request_id=request_id, indexes=indexes)

    def hear(self, frame):
        self.chat._on_frame(0, frame)

    def test_initial_request_is_one_small_window(self):
        self.assertGreater(len(self.frames()), 30)
        self.assertEqual(self.chat._request_thumbnail(self.attachment, attempt=0), 1)
        self.assertEqual(len(self.chat.data.sent), 1)
        self.assertEqual(self.chat.data.sent[0]['i'], list(range(IMAGE_REQUEST_WINDOW)))

    def test_larger_sequence_numbers_reduce_window_to_one_legal_request_frame(self):
        self.chat.store.set_attachment(self.origin, 2147483647, self.digest,
                                       len(self.body), 160, 120)
        attachment = self.chat.store.attachment_for(self.origin, 2147483647)
        self.chat._request_thumbnail(attachment, attempt=0)
        self.assertEqual(len(self.chat.data.sent), 1)
        request = self.chat.data.sent[0]
        self.assertLess(len(request['i']), IMAGE_REQUEST_WINDOW)
        self.assertLessEqual(len(pack(request)), 220)

    def test_next_window_waits_for_previous_chunks_then_finishes_exact_bytes(self):
        self.chat._request_thumbnail(self.attachment, attempt=0)
        requests = 0
        while self.chat.data.sent:
            request = self.chat.data.sent.pop(0)
            self.assertEqual(request['t'], 'g')
            self.assertLessEqual(len(request['i']), IMAGE_REQUEST_WINDOW)
            for frame in self.frames(indexes=request['i'], request_id=request['id']):
                self.hear(frame)
            requests += 1
            self.assertLess(requests, 25)
        self.assertEqual(self.chat.store.image_for(self.digest), self.body)
        self.assertNotIn(self.key, self.chat.pending_images)
        self.assertNotIn(self.key, self.chat.image_windows)

    def test_retry_requests_only_lost_chunks_from_active_window_and_keeps_expiry(self):
        self.chat._request_thumbnail(self.attachment, attempt=0)
        original_expiry = self.chat.pending_images[self.key][2]
        for frame in self.frames(indexes=list(range(1, IMAGE_REQUEST_WINDOW))):
            self.hear(frame)
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=time.monotonic() + IMAGE_RESPONSE_SECONDS + 1):
            self.chat._retry_pending_images()
        self.assertEqual(len(self.chat.data.sent), 1)
        self.assertEqual(self.chat.data.sent[0]['i'], [0])
        self.assertEqual(self.chat.pending_images[self.key][2], original_expiry)

    def test_new_chunk_extends_silence_timer_without_bursting_repairs(self):
        now = time.monotonic()
        with patch('kisschat.app.time.monotonic', return_value=now):
            self.chat._request_thumbnail(self.attachment, attempt=0)
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=now + IMAGE_RETRY_SECONDS - 5):
            self.hear(self.frames(indexes=[0])[0])
        with patch('kisschat.app.time.monotonic', return_value=now + IMAGE_RETRY_SECONDS + 1):
            self.chat._retry_pending_images()
        self.assertEqual(self.chat.data.sent, [])

    def test_overhearer_caches_and_announces_image_to_ui_without_requesting_repairs(self):
        for frame in self.frames(recipient='04' * 16):
            self.hear(frame)
        self.assertEqual(self.chat.store.image_for(self.digest), self.body)
        self.assertFalse(any(r['t'] == 'g' for r in self.chat.data.sent))
        events = list(self.chat.events.items)
        self.assertIn(('image', {'o': self.origin, 's': 1}), events)
        self.assertEqual(self.chat.pending_images, {})

    def test_sender_does_not_echo_its_own_returned_chunks_or_store_partial_copy(self):
        self.assertTrue(self.chat.store.put_image(self.digest, self.body))
        self.chat._send_image('04' * 16, self.origin, 1, self.digest,
                              self.body, '12345678', [0, 1, 2])
        frames = list(self.chat.data.sent)
        self.chat.data.sent.clear()
        for record in frames:
            self.hear(image_part(recipient=record['to'], origin=record['o'],
                                 seq=record['s'], digest=record['h'], index=record['i'],
                                 count=record['z'], chunk=record['b'], request_id=record['id'], ttl=3))
        self.assertEqual(self.chat.data.sent, [])
        self.assertEqual(self.chat.store.db.execute('SELECT COUNT(*) FROM image_fragments').fetchone()[0], 0)

    def test_complete_cache_serves_a_window_without_flooding_the_request_farther(self):
        self.assertTrue(self.chat.store.put_image(self.digest, self.body))
        with patch.object(self.chat, '_schedule_image_response') as respond:
            self.hear(pack(dict(t='g', p='04' * 16, o=self.origin, s=1, h=self.digest,
                                id='12345678', ttl=3, i=[0, 1, 2])))
        respond.assert_called_once()
        self.assertEqual(self.chat.data.sent, [])

    def test_receiver_defers_background_announcements_to_window_boundary(self):
        self.chat._request_thumbnail(self.attachment, attempt=0)
        self.chat.data.sent.clear()
        self.chat.pending_announcements[self.origin] = time.monotonic() - 1
        self.chat._flush_relay_announcements()
        self.assertEqual(self.chat.data.sent, [])
        for frame in self.frames(indexes=list(range(IMAGE_REQUEST_WINDOW))):
            self.hear(frame)
        self.assertEqual([r['t'] for r in self.chat.data.sent], ['i', 'g'])

    def test_receiver_does_not_broadcast_the_responder_claim_back(self):
        self.hear(pack(dict(t='y', p=self.chat.store.node, o=self.origin, s=1,
                            h=self.digest, id='12345678', by='04' * 16, ttl=3)))
        self.assertEqual(self.chat.data.sent, [])

    def test_responder_claim_extends_the_receive_silence_timer(self):
        now = time.monotonic()
        with patch('kisschat.app.time.monotonic', return_value=now):
            self.chat._request_thumbnail(self.attachment, attempt=0)
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=now + 15):
            self.hear(pack(dict(t='y', p=self.chat.store.node, o=self.origin, s=1,
                                h=self.digest, id='12345678', by='04' * 16, ttl=3)))
        with patch('kisschat.app.time.monotonic', return_value=now + IMAGE_RETRY_SECONDS + 1):
            self.chat._retry_pending_images()
        self.assertEqual(self.chat.data.sent, [])
        self.assertEqual(self.chat.pending_images[self.key][1], now + 15 + IMAGE_RETRY_SECONDS)

    def test_first_request_allows_routed_claim_to_arrive_before_retrying(self):
        now = time.monotonic()
        with patch('kisschat.app.time.monotonic', return_value=now):
            self.chat._request_thumbnail(self.attachment, attempt=0)
        self.chat.data.sent.clear()
        with patch('kisschat.app.time.monotonic', return_value=now + 30):
            self.chat._retry_pending_images()
        self.assertEqual(self.chat.data.sent, [])


if __name__ == '__main__':
    unittest.main()
