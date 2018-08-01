import asyncio
import struct
import traceback
import bitstring


class Peer():
    def __init__(self, session, host, port):
        self.host = host
        self.port = port
        self.session = session

        self.have_pieces = bitstring.BitArray(
            bin='0' * self.session.number_of_pieces
        )
        self.piece_in_progress = None
        self.blocks = None

        self.inflight_requests = 0

    def handshake(self):
        return struct.pack(
            '>B19s8x20s20s',
            19,
            b'BitTorrent protocol',
            self.session.info_hash,
            "a1b2c3d4e5f6g7h8i9j0".encode()
        )

    async def send_interested(self, writer):
        msg = struct.pack('>Ib', 1, 2)
        writer.write(msg)
        print("Before draining writer in send interested for {}".format(self.host))
        await writer.drain()
        print("After draining writer in send interested for {}".format(self.host))

    def get_blocks_generator(self):
        def blocks():
            while True:
                try:
                    piece = self.session.get_piece_request(self.have_pieces)
                    print('[{}] Generating blocks for Piece: {}'.format(self, piece))
                    for block in piece.blocks:
                        yield block
                except Exception as e:
                    print("No piece available from this peer")
                    return
        if not self.blocks:
            self.blocks = blocks()
        return self.blocks

    async def request_a_piece(self, writer):
        if self.inflight_requests > 1:
            return
        blocks_generator = self.get_blocks_generator()
        block  = next(blocks_generator)
        if not block:
            print("No blocks generated")
            return

        msg = struct.pack('>IbIII', 13, 6, block.piece, block.begin, block.length)
        writer.write(msg)
        self.inflight_requests += 1
        print("Before draining writer in request a piece for {}".format(self.host))
        await writer.drain()
        print("After draining writer in request a piece for {}".format(self.host))

    async def download(self):
        retries = 0
        while retries < 5:
            retries += 1
            try:
                print("Before awaiting self._download for {}".format(self.host))
                await self._download()
                print("After awaiting self._download for {}".format(self.host))
            except Exception as e:
                print('Error downloading: {}\n\n'.format(self.host))
                self.inflight_requests -= 1
                traceback.print_exc()

    async def _download(self):
        try:
            reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=10
            )

        except Exception as e:
            print('Failed to connect to Peer {}\n\n'.format(self.host, e))
            self.inflight_requests -= 1
            traceback.print_exc()
            return

        print('{} Sending handshake'.format(self.host))
        writer.write(self.handshake())
        print("Before draining writer for {}".format(self.host))
        await writer.drain()
        print("After draining writer for {}".format(self.host))

        try:
            handshake = await asyncio.wait_for(reader.read(68), timeout=5)  # Suspends here if there's nothing to be read
        except Exception as e:
            print('Failed at handshake to Peer {}\n\n'.format(self.host))
            self.inflight_requests -= 1
            traceback.print_exc()
            return

        try:
            await self.send_interested(writer)
        except Exception as e:
            print('Failed at sending interested to Peer {}\n\n'.format(self.host))
            self.inflight_requests -= 1
            traceback.print_exc()
            return

        buf = b''
        while True:
            try:
                resp = await asyncio.wait_for(reader.read(16384), timeout=60)
            except Exception as e:
                print('Failed at Reading data from Peer {}\n\n'.format(self.host))
                self.inflight_requests -= 1
                traceback.print_exc()
                return

            buf += resp

            if not buf and not resp:
                return

            while True:
                if len(buf) < 4:
                    break

                length = struct.unpack('>I', buf[0:4])[0]

                if not len(buf) >= length:
                    break

                def consume(buf):
                    buf = buf[4 + length:]
                    return buf

                def get_data(buf):
                    return buf[:4 + length]


                if length == 0:
                    print('[Message] Keep Alive')
                    buf = consume(buf)
                    data = get_data(buf)
                    # print('[DATA]', data)
                    continue

                if len(buf) < 5:
                    # print('Buffer is less than 5... breaking')
                    break

                msg_id = struct.unpack('>b', buf[4:5])[0] # 5th byte is the ID

                if msg_id == 0:
                    print('[Message] CHOKE')
                    data = get_data(buf)
                    buf = consume(buf)
                    # print('[DATA]', data)

                elif msg_id == 1:
                    data = get_data(buf)
                    buf = consume(buf)
                    print('[Message] UNCHOKE')
                    self.peer_choke = False

                elif msg_id == 2:
                    data = get_data(buf)
                    buf = consume(buf)
                    print('[Message] Interested')
                    pass

                elif msg_id == 3:
                    data = get_data(buf)
                    buf = consume(buf)
                    print('[Message] Not Interested')
                    pass

                elif msg_id == 4:
                    buf = buf[5:]
                    data = get_data(buf)
                    buf = consume(buf)
                    print('[Message] Have')
                    pass

                elif msg_id == 5:
                    bitfield = buf[5: 5 + length - 1]
                    self.have_pieces = bitstring.BitArray(bitfield)
                    buf = buf[4 + length:]
                    await self.send_interested(writer)

                elif msg_id == 7:
                    self.inflight_requests -= 1
                    data = get_data(buf)
                    buf = consume(buf)

                    l = struct.unpack('>I', data[:4])[0]
                    try:
                        parts = struct.unpack('>IbII' + str(l - 9) + 's', data[:length + 4])
                        piece_idx, begin, data = parts[2], parts[3], parts[4]
                        self.session.on_block_received(piece_idx, begin, data)
                    except struct.error:
                        print('error decoding piece')
                        return None

                else:
                    print('unknown ID {}'.format(msg_id))
                    if msg_id == 159:
                        exit(1)

                try:
                    await self.request_a_piece(writer)
                except Exception as e:
                    print('{} Failed at requesting a piece\n\n'.format(self.host))
                    self.inflight_requests -= 1
                    traceback.print_exc()
                    return

    def __repr__(self):
        return '[Peer {}:{}]'.format(self.host, self.port)
