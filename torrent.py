import asyncio
import math
import os
import random
import socket
import string
import struct
from hashlib import sha1
from pprint import pformat

import bencoding
import requests
from pybtracker import TrackerClient


# TODO Decode the whole thing to a map of python str
class Torrent:
    """
    # TODO Move torrent and Magnet to a single common Metadata source module
    # TODO Add mainline DHT
    # TODO Write adapters to create a common format data out of

    Representation of the metadata from bdecoding a Torrent file
    """
    def __init__(self, file_path):
        if os.path.isfile(file_path) and file_path.split('.')[-1] == 'torrent':
            with open(file_path, 'rb') as f:
                self.metaData = bencoding.bdecode(f.read())
        else:
            raise ValueError('Invalid torrent file')

        self._announce = self.metaData[b'announce']

        if b'private' in self.metaData[b'info']:
            self._isPrivate = True if int(self.metaData[b'info'][b'private']) == 1 else False
        else:
            self._isPrivate = False

        self._pieces = self.metaData[b'info'][b'pieces']

        self._piece_length = self.metaData[b'info'][b'piece length']

        if b'announce-list' not in self.metaData:
            self._trackers = [self._announce]
        else:
            self._trackers = self.metaData[b'announce-list']
            self._trackers = [tracker for sublist in self._trackers for tracker in sublist if b'ipv6' not in tracker]

        if b'creation date' in self.metaData:
            self._creationDate = self.metaData[b'creation date']

        if b'comment' in self.metaData:
            self._comment = self.metaData[b'comment']

        if b'created by' in self.metaData:
            self._createdBy = self.metaData[b'created by']

        if b'encoding' in self.metaData:
            self._encoding = self.metaData[b'encoding']

        if b'files' not in self.metaData[b'info']:
            self.mode = 'single'
            self._total_length = self.metaData[b'info'][b'length']
            if b'md5sum' in self.metaData:
                self._md5sum = self.metaData[b'info'][b'md5sum']
        else:
            self.mode = 'multiple'
            self.files = self.metaData[b'info'][b'files']
            # Man Made stuff here onwards

            # self.Fractures stores File finish indexes
            self.files, self._total_length, self.fractures = self.__parse_files()

        self.number_of_pieces = math.ceil(self._total_length / self._piece_length)

        print("MODE:", self.mode)
        print("TOTAL LENGTH:", self._total_length)
        print("PIECE_LEN:", self._piece_length)
        print("NO. OF PIECES:", self.number_of_pieces)
        print("LAST PIECE LEN:", self._total_length % self._piece_length)
        print("NO. OF PIECE HASHES", len(self._pieces)/20)

        self.name = self.metaData[b'info'][b'name']  # Usage depends on mode

        self.info_hash = sha1(bencoding.bencode(self.metaData[b'info'])).digest()

        self.peers = []
        # await self._get_peers()

        # print(self.peers)

    def get_piece_hash(self, piece_idx):
        """
        Gets the hash of a particular piece using piece index, from a concatenated string of SHA1 hashes of all pieces
        :param piece_idx: piece index
        :return: piece hash
        """
        return self._pieces[piece_idx*20: (piece_idx*20) + 20]

    def __parse_files(self):
        """
        Parses metaData[b'info'][b'files'] in case of multiple file mode
        :return: parsed_files list, total length of all files and fracture indexes
        """
        parsed_files = []
        fractures = []
        total_length = 0
        for file in self.files:
            file_length = file[b'length']
            file_path = file[b'path']
            if b'md5sum' in file:
                file_md5sum = file[b'md5sum']
                parsed_files.append({b'length': file_length, b'path': file_path, b'md5sum': file_md5sum})
            else:
                parsed_files.append({b'length': file_length, b'path': file_path})

            total_length += file_length
            fractures.append(total_length)
            # print(total_length)

        return parsed_files, total_length, fractures

    async def udp_tracker_client(self, url):
        """
        UDP Tracker client implementation
        :param url: tracker url
        :return: peers
        """
        client = TrackerClient(announce_uri=url)
        await asyncio.wait_for(client.start(), timeout=10)
        peers = await asyncio.wait_for(client.announce(
            self.info_hash,  # infohash
            0,  # downloaded
            self._total_length,  # left
            0,  # uploaded
            0,  # event (0=none)
            120  # number of peers wanted
        ), timeout=10)
        print("UDP TRACKER PEERS:", peers)
        return peers

    async def get_peers(self, numwant=100):
        """
        # TODO Separate out metadata parsing and HTTP client implementation
        HTTP(S) Tracker client implementation
        :param numwant: required number of peers
        """
        peer_id = 'SA' + ''.join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(18)
            )
        params = {
            'info_hash': self.info_hash,
            'peer_id': peer_id,
            'port': 6881,
            'uploaded': 0,
            'downloaded': 0,
            'left': self._total_length,
            'compact': 1,
            'no_peer_id': 1,
            'event': 'started',
            'numwant': numwant
            }
        for url in self._trackers:
            if b'udp' not in url:
                try:
                    print(url)
                    r = requests.get(url, params=params, timeout=10)
                except Exception as e:
                    print("Exception occurred for {}\n{}".format(url, e))
                    continue
                resp = bencoding.bdecode(r.content)
                print(r.status_code, r.reason)
                peers = resp[b'peers']
                start = 0

                if isinstance(peers, list):
                    self.peers = peers
                elif len(peers) % 6 == 0:
                    while start < len(peers):
                        ip = peers[start:start+4]
                        ip = socket.inet_ntoa(ip)
                        port = peers[start+4:start+6]
                        port, = struct.unpack('!H', port)
                        self.peers.append((ip, port))
                        start += 6
            else:
                try:
                    print(url)
                    udp_peers = await self.udp_tracker_client(url.decode())
                    self.peers.extend(udp_peers)
                except Exception as e:
                    print("Exception occurred for {}\n{}".format(url, e))
                    continue

    def __str__(self):
        return pformat(self.metaData)
