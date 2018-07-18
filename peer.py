import asyncio
import struct
from collections import defaultdict
import traceback
import bitstring


class Peer():
    def __init__(self, session, host, port):
        self.host = host
        self.port = port
        self.session = session

        # Pieces this torrent is able to serve us
        self.have_pieces = bitstring.BitArray(
            bin='0' * self.session.number_of_pieces
        )
        self.being_used = False
        self.blocks = None

    def handshake(self):
        return struct.pack(
            '>B19s8x20s20s',
            19,
            b'BitTorrent protocol',
            self.session.info_hash,
            "a1b2c3d4e5f6g7h8i9j0".encode()
        )

    async def send_interested(self, writer):
        # TODO: refactor into messages util
        msg = struct.pack('>Ib', 1, 2)
        writer.write(msg)
        await writer.drain()

    def get_blocks_generator(self, piece):
        def blocks():
            while True:
                try:
                    print('[{}] Generating blocks for Piece: {}'.format(self, piece))
                    for block in piece.blocks:
                        yield block
                except Exception as e:
                    print("No piece available from this peer")
                    return
        if not self.blocks:
            self.blocks = blocks()
        return self.blocks


    async def request_a_piece(self, writer, piece):
        """
        Generate a block for the piece provided and request it from the peer
        """
        if self.being_used:
            print("{} Peer busy, Not generating blocks".format(self.host))
            return None
        blocks_generator = self.get_blocks_generator(piece)
        block  = next(blocks_generator)
        if not block:
            print("No blocks generated")
            return None

        msg = struct.pack('>IbIII', 13, 6, block.piece, block.begin, block.length)
        writer.write(msg)
        self.being_used = True
        await writer.drain()

    async def get_bitfield(self):
        retries = 0
        while retries < 5:
            retries += 1
            try:
                res = await self._get_bitfield()
                return res
            except Exception as e:
                print('Error getting bitfield: {}\n\n'.format(self.host))
                self.being_used = False
                traceback.print_exc()
                return None

    async def _get_bitfield(self):
        try:
            reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=10
            )

        except Exception as e:
            print('Failed to connect to Peer {}\n\n'.format(self.host, e))
            self.being_used = False
            traceback.print_exc()
            return

        print('{} Sending handshake'.format(self))
        writer.write(self.handshake())
        await writer.drain()

        try:
            handshake = await asyncio.wait_for(reader.read(68), timeout=5)  # Suspends here if there's nothing to be read
        except Exception as e:
            print('Failed at handshake to Peer {}\n\n'.format(self.host))
            self.being_used = False
            traceback.print_exc()
            return None

        try:
            await self.send_interested(writer)
        except Exception as e:
            print('Failed at sending interested to Peer {}\n\n'.format(self.host))
            self.being_used = False
            traceback.print_exc()
            return None

        buf = b''
        while True:
            try:
                resp = await asyncio.wait_for(reader.read(16384), timeout=5)  # Suspends here if there's nothing to be read
            except Exception as e:
                print('Failed at Reading data from Peer {}\n\n'.format(self.host))
                self.being_used = False
                traceback.print_exc()
                return None

            buf += resp

            if not buf and not resp:
                print("NOT BUFFER AND NOT RESPONSE")
                return None

            while True:
                if len(buf) < 4:
                    print('Buffer is too short', len(buf))
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
                    continue

                if len(buf) < 5:
                    print('Buffer is less than 5... breaking')
                    break

                msg_id = struct.unpack('>b', buf[4:5])[0] # 5th byte is the ID

                if msg_id == 0:
                    print('[Message] CHOKE')
                    data = get_data(buf)
                    buf = consume(buf)

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

                    return self.have_pieces

                else:
                    print('unknown ID {}'.format(msg_id))
                    if msg_id == 159:
                        exit(1)
                    return None

    async def download(self, piece):
        retries = 0
        while retries < 5:
            retries += 1
            try:
                await self._download(piece)
            except Exception as e:
                print('Error downloading: {}\n\n'.format(self.host))
                self.being_used = False
                traceback.print_exc()

    async def _download(self, piece):
        try:
            reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=10
            )

        except Exception as e:
            print('Failed to connect to Peer {}\n\n'.format(self.host, e))
            self.being_used = False
            traceback.print_exc()
            return

        print('{} Sending handshake'.format(self))
        writer.write(self.handshake())
        await writer.drain()

        try:
            handshake = await asyncio.wait_for(reader.read(68), timeout=5)  # Suspends here if there's nothing to be read
        except Exception as e:
            print('Failed at handshake to Peer {}\n\n'.format(self.host))
            self.being_used = False
            traceback.print_exc()
            return

        try:
            await self.send_interested(writer)
        except Exception as e:
            print('Failed at sending interested to Peer {}\n\n'.format(self.host))
            self.being_used = False
            traceback.print_exc()
            return

        buf = b''
        while True:
            try:
                resp = await asyncio.wait_for(reader.read(16384), timeout=5)  # Suspends here if there's nothing to be read
            except Exception as e:
                print('Failed at Reading data from Peer {}\n\n'.format(self.host))
                self.being_used = False
                traceback.print_exc()
                return

            buf += resp

            if not buf and not resp:
                print("NOT BUFFER AND NOT RESPONSE")
                return

            while True:
                if len(buf) < 4:
                    print('Buffer is too short', len(buf))
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
                    continue

                if len(buf) < 5:
                    print('Buffer is less than 5... breaking')
                    break

                msg_id = struct.unpack('>b', buf[4:5])[0] # 5th byte is the ID

                if msg_id == 0:
                    print('[Message] CHOKE')
                    data = get_data(buf)
                    buf = consume(buf)

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
                    data = get_data(buf)
                    buf = consume(buf)

                    l = struct.unpack('>I', data[:4])[0]
                    try:
                        self.being_used = True
                        parts = struct.unpack('>IbII' + str(l - 9) + 's', data[:length + 4])
                        piece_idx, begin, data = parts[2], parts[3], parts[4]
                        self.session.on_block_received(piece_idx, begin, data)
                        print('Got piece idx {} - Block begin idx {}'.format(piece_idx, begin))
                        self.being_used = False
                    except struct.error:
                        print('error decoding piece')
                        self.being_used = False
                        return None

                else:
                    print('unknown ID {}'.format(msg_id))
                    if msg_id == 159:
                        exit(1)

                try:
                    await self.request_a_piece(writer, piece)
                except Exception as e:
                    print('{} Failed at requesting a piece\n\n'.format(self.host))
                    self.being_used = False
                    traceback.print_exc()
                    return

    def __repr__(self):
        return '[Peer {}:{}]'.format(self.host, self.port)
