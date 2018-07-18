import asyncio
import bitstring
import hashlib
import math
import sys
from typing import Dict, List
from pprint import pformat, pprint
import os
from file_saver import FileSaver
from peer import Peer
from torrent import Torrent
from pdb import set_trace as brkpt
import warnings
from tqdm import tqdm


class Piece(object):
    def __init__(self, index : int, blocks : list, file_name : str, file_idx : int, in_conflict : bool, fracture_idx : int):
        self.index : int = index
        self.blocks : list = blocks
        self.downloaded_blocks : bitstring.BitArray = \
            bitstring.BitArray(bin='0'*len(blocks))
        self.in_conflict : bool = in_conflict
        self.fracture_idx : int = fracture_idx
        self.file_name : str = file_name
        self.file_idx : int = file_idx

    def flush(self):
        [block.flush() for block in self.blocks]

    def is_complete(self) -> bool:
        """
        Return True if all the Blocks in this piece exist
        """
        return all(self.downloaded_blocks)

    def save_block(self, begin : int, data : bytes):
        """
        Writes block 'data' into block object
        """
        for block_idx, block in enumerate(self.blocks):
            if block.begin == begin:
                block.data = data
                self.downloaded_blocks[block_idx] = True

    @property
    def data(self) -> bytes:
        """
        Returns Piece data
        """
        return b''.join([block.data for block in self.blocks])

    @property
    def hash(self):
        return hashlib.sha1(self.data)

    def __repr__(self):
        return '<Piece: {} Blocks: {} In Conflict: {} File Index: {} File Name: {} Fracture Point {}>'.format(
            self.index,
            len(self.blocks),
            self.in_conflict,
            self.file_idx,
            self.file_name,
            self.fracture_idx
        )


class Block(object):
    def __init__(self, piece, begin, length):
        self.piece = piece
        self.begin = begin
        self.length = length
        self.data = None

    def flush(self):
        self.data = None

    def __repr__(self):
        return '[Block ({}, {}, {})]'.format(
            self.piece,
            self.begin,
            self.length
        )


class DownloadSession(object):
    def __init__(self, torrent : Torrent, writer : asyncio.Queue = None):
        # self.writer = writer
        self.torrent : Torrent = torrent
        self.piece_size : int = self.torrent._metaData[b'info'][b'piece length']
        self.number_of_pieces : int = self.torrent.number_of_pieces
        if self.torrent._mode == 'multiple':
            self.fractures = self.torrent.fractures
            print("DLSESSION", self.torrent._mode, self.fractures)
            self.file_names = [os.path.join(*file[b'path']).decode() for file in self.torrent._files]  # Files list for popping in order, then processed path key to get final name
            # print(self.file_names)
        
        self.pieces : list = self.get_pieces()
        self.pieces_in_progress : Dict[int, Piece] = {}  # NOT USED  ####TEST####
        self.received_pieces : Dict[int, Piece]= {}  ####TEST####
        self.received_pieces_queue : asyncio.Queue = writer
        self.info_hash = self.torrent._info_hash


    def on_block_received(self, piece_idx, begin, data):
        """
        1. Removes piece from self.pieces
        2. Verifies piece hash
        3. Sets self.have_pieces[piece.index] = True if hash is valid
        4. Else re-inserts piece into self.pieces
        :return:  None
        """

        piece = self.pieces[piece_idx]
        piece.save_block(begin, data)

        # Verify all blocks in the Piece have been downloaded
        if not piece.is_complete():
            # print('Piece not complete')
            return

        piece_data = piece.data

        res_hash = hashlib.sha1(piece_data).digest()
        exp_hash = self.torrent.get_piece_hash(piece.index)

        if res_hash != exp_hash:
            del self.pieces_in_progress[piece_idx]  # Not in progress anymore
            print('Hash check failed for Piece {}'.format(piece.index))
            piece.flush()
            return
        else:
            self.received_pieces[piece_idx] = piece
            print('Piece {} hash is valid'.format(piece.index))
            print('Piece {} DL'.format(piece.index))

        # Only runs when a piece is complete
        # Double braces because one set is for the tuple being sent
        self.received_pieces_queue.put_nowait((piece.index * self.piece_size, piece.file_idx, piece_data, piece.in_conflict, piece.fracture_idx, piece.file_name, piece))

    def get_pieces(self) -> list:
        """
        Generates list of pieces and their blocks
        """

        ####FILE_ITER is the file's number####
        ####FILE_IDX is the piece's index inside its file####
        pieces = []
        blocks_per_piece = math.ceil(self.piece_size / 16384)
        file_idx = 0
        file_iter = 0
        fracture = 0
        for piece_idx in tqdm(range(self.number_of_pieces)):
            file_name = ""
            blocks = []
            outcome = False
            # brkpt()
            if self.torrent._mode == 'multiple':
                piece_end = piece_idx * self.piece_size + self.piece_size
                piece_beg = piece_idx * self.piece_size
                file_idx = piece_beg - fracture  # Piece's absolute index - previous fracture point i.e previous files' length
                if len(self.fractures) > 1:  # Probabaly not needed  ####TEST####
                    if self.fractures[file_iter] <= piece_end:
                        if self.fractures[file_iter] >= piece_beg:
                            # Piece ends after fracture point and also starts before fracture point, therefore the piece is in conflict
                            print('Fracture found in piece {} at {}'.format(piece_idx, fracture))
                            outcome = True
                            file_name = self.file_names[file_iter] + '|' + self.file_names[file_iter + 1]  # Assigning file names for both files, existing in the piece in conflict, concatenated with a '|' pipe
                            fracture = self.fractures[file_iter]
                            file_iter += 1
                        else:
                            # Piece ends after fracture point but does not start before fracture, therefore belongs to a future file and getting here is an ANOMALY
                            print("[ERROR] FUTURE FILE PIECE ANOMALY")
                    else:
                        # Piece ends before fracture point, going in order, so belongs to current file
                        file_name = self.file_names[file_iter]
                elif len(self.fractures) == 1:
                    print('Last fracture is at the end of data.')
                    print('Last fracture:', self.fractures[-1])
                else:
                    print('No fractures left in the list')

            for block_idx in range(blocks_per_piece):
                is_last_block = (blocks_per_piece - 1) == block_idx
                block_length = (
                    (self.piece_size % 16384) or 16384
                    if is_last_block
                    else 16384
                )
                blocks.append(
                    Block(
                        piece_idx,
                        block_length * block_idx,
                        block_length
                    )
                )

            this_piece = Piece(piece_idx, blocks, file_name, file_idx, outcome, fracture)
            # print(this_piece)
            pieces.append(this_piece)
        return pieces

    def get_piece_request(self, have_pieces):
        """
        Determines next piece for downloading. Expects BitArray
        of pieces a peer can request
        """
        for piece in self.pieces:  # Synchronous if available
            # Don't create request out of pieces we already have
            is_piece_downloaded = piece.index in self.received_pieces
            is_piece_in_progress = piece.index in self.pieces_in_progress

            # Skip pieces we already have
            if is_piece_downloaded or is_piece_in_progress:
                print('IDX {} is_piece_downloaded {}'.format(piece.index, is_piece_downloaded))
                print('IDX {} is_piece_in_progress {}'.format(piece.index, is_piece_in_progress))
                continue

            if have_pieces[piece.index]:
                self.pieces_in_progress[piece.index] = piece
                print("Piece {} PR".format(piece.index))
                return piece
        # raise Exception('Not eligible for valid pieces')

    def __repr__(self):
        data = {
            'number of pieces': self.number_of_pieces,
            'piece size': self.piece_size,
            'pieces': self.pieces[:5]
        }
        return pformat(data)


class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)


async def download(torrent_file : str, download_location : str, loop=None):
    torrent = Torrent(torrent_file)

    torrent_writer = FileSaver(download_location, torrent)
    session = DownloadSession(torrent, torrent_writer.get_received_pieces_queue())  # FILESAVER

    peers_info = torrent.peers  # ASYNCIFY THIS using await

    seen_peers = set()

    # peers = [Peer(session, host, port) for host, port in peers_info]

    alive_peers = await (asyncio.gather(*[Peer.create(session, host, port) for host, port in peers_info if not None]))

    seen_peers.update([str(p) for p in alive_peers])

    print('[Peers]: {} {}'.format(len(seen_peers), seen_peers))

    await (asyncio.gather(*[peer.download() for peer in alive_peers if peer.inflight_requests < 1 and peer.have_pieces != None]))

    piece_ledger = bitstring.BitArray(
        bin='0' * torrent.number_of_pieces
    )


    # pprint(pieces_got)


if __name__ == '__main__':
    f = open('logfile', 'w')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    loop = asyncio.get_event_loop()
    # For Debugging
    # loop.set_debug(True)
    # loop.slow_callback_duration = 0.001
    # warnings.simplefilter('always', ResourceWarning)
    loop.run_until_complete(download(sys.argv[1], './downloads', loop=loop))
    loop.close()
