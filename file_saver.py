import asyncio
import os


class FileSaver:
    """
    # TODO Implement async write and replace Queue worker with just callback to producer task completions
    File saver worker(consumer) to pop pieces(topics) and write them synchronously
    """
    def __init__(self, outdir, torrent):
        self.torrent = torrent
        self.file_path = os.path.join(outdir, torrent.name.decode())
        self.file_name = ""
        if self.torrent.mode == 'multiple':
            if not os.path.isdir(self.file_path):
                print("Creating dir", self.file_path)
                os.mkdir(self.file_path)
                # Name in multiple mode becomes directory name, so created that directory if it does not exist

        else:
            # single file mode
            self.fd = os.open(self.file_path,
                              os.O_RDWR | os.O_CREAT)  # File_Path is the File_Name in the single file mode

        self.received_pieces_queue = asyncio.Queue()
        asyncio.ensure_future(self.write())

    def get_received_pieces_queue(self):
        """
        Interface to expose the completed pieces queue
        :return: queue
        """
        return self.received_pieces_queue

    async def write(self):
        """
        # TODO Separate out worker and writing corouting
           And handle errors by requeuing the piece and not performing cleanups, if cleaned up, update piece DL status
        Piece writing coroutine with an infinite blocking worker
        """
        while True:
            piece = await asyncio.wait_for(self.received_pieces_queue.get(), timeout=5)
            if not piece:
                print("Poison pill. Exiting")

            piece_abs_location, file_idx, piece_data, in_conflict, fracture, file_name, piece_instance = piece
            # piece_abs_location to be changed to file's index
            print("Writing a Piece")
            print("Name: {} Conflicted: {} PIECE ABS LOCATION: {} FRACTURE POINT: {} LOCATION IN FILE: {}".format(
                file_name, in_conflict, piece_abs_location, fracture,
                file_idx))  # Don't print piece_data for the sake of readability

            # HANDLE THE SINGLE FILE CASE SEPARATELY FROM NON CONFLICT
            if self.torrent.mode == 'single':
                os.lseek(self.fd, piece_abs_location, os.SEEK_SET)
                # Piece index is the File index in case of single file
                os.write(self.fd, piece_data)
                piece_instance.flush()  # Remove from RAM after writing to disk
                print("Piece {} WR".format(piece_instance.index))
            else:
                if not in_conflict:
                    self.file_name = os.path.join(self.file_path, file_name)
                    # File_name won't change so created beforehand
                    self.fd = os.open(self.file_name, os.O_RDWR | os.O_CREAT)
                    os.lseek(self.fd, file_idx, os.SEEK_SET)  # FIND FILE INDEX FOR THE PIECE IN ITS FILE
                    os.write(self.fd, piece_data)
                    piece_instance.flush()  # Remove from RAM after writing to disk
                    os.close(self.fd)
                    print("Piece {} WR".format(piece_instance.index))
                else:
                    print("Writing a fractured piece")
                    current_file, next_file = file_name.split('|')
                    print(current_file, next_file)
                    # File names are changing, so creation on the fly

                    # FIRST FRAGMENT
                    self.fd = os.open(os.path.join(self.file_path, current_file), os.O_RDWR | os.O_CREAT)
                    os.lseek(self.fd, file_idx, os.SEEK_SET)  # Go to the File index for the piece
                    os.write(self.fd, piece_data[:(fracture - piece_abs_location)])
                    # Write first fragment of the piece from beg upto length of \
                    # first fragment (fracture point - piece_abs_location)
                    os.close(self.fd)
                    # SECOND FRAGMENT
                    self.fd = os.open(os.path.join(self.file_path, next_file), os.O_RDWR | os.O_CREAT)
                    os.lseek(self.fd, 0, os.SEEK_SET)  # Go to beginning of the next file
                    os.write(self.fd, piece_data[(fracture - piece_abs_location):])
                    # Write second fragment of the piece from fracture point to end
                    piece_instance.flush()  # Remove from RAM after writing to disk
                    os.close(self.fd)
                    print("Piece {} WR".format(piece_instance.index))
                    # Two fragments for one piece therefore only logged as written once
