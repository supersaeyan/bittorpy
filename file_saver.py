import os


class FileSaver(object):
    def __init__(self, outdir, torrent):
        self.torrent = torrent
        self.file_path = os.path.join(outdir, torrent._name.decode())
        if self.torrent._mode == 'multiple':
            if not os.path.isdir(self.file_path):
                print("Creating dir", self.file_path)
                os.mkdir(self.file_path)  # Name in multiple mode becomes directory name, so created that directory if it does not exist

        # self.fd = os.open(self.file_name, os.O_RDWR | os.O_CREAT)
        self.received_blocks_queue = []

    def get_received_blocks_queue(self):
        return self.received_blocks_queue

    def write(self, piece):
        ####FIND FILE INDEXES OF PIECES AND WRITE TO FILES####
        piece_abs_location, file_idx, piece_data, in_conflict, fracture, file_name, piece_instance = piece  # piece_abs_location to be changed to file's index
        print("Writing a Piece")
        print(file_name, in_conflict, piece_abs_location, fracture, file_idx) # Don't print piece_data for the sake of readibility

        ####HANDLE THE SINGLE FILE CASE SEPERATELY FROM NON CONFLICT####
        if self.torrent._mode == 'single':
            self.fd = os.open(self.file_path, os.O_RDWR | os.O_CREAT)  # File_Path is the File_Name in the single file mode
            os.lseek(self.fd, piece_abs_location, os.SEEK_SET)  # Piece index is the File index in case of single file
            os.write(self.fd, piece_data)
            piece_instance.flush()  # Remove from RAM after writing to disk
        else:
            if not in_conflict:
                self.file_name = os.path.join(self.file_path, file_name)  # File_name won't change so created beforehand 
                self.fd = os.open(self.file_name, os.O_RDWR | os.O_CREAT)
                os.lseek(self.fd, file_idx, os.SEEK_SET)  ####FIND FILE INDEX FOR THE PIECE IN ITS FILE####
                os.write(self.fd, piece_data)
                piece_instance.flush()  # Remove from RAM after writing to disk
            else:
                print("Writing a fractured piece")
                current_file, next_file = file_name.split('|')
                print(current_file, next_file)
                # File names are changing, so creation on the fly

                ####FIRST FRAGMENT####
                self.fd = os.open(os.path.join(self.file_path, current_file), os.O_RDWR | os.O_CREAT)
                os.lseek(self.fd, file_idx, os.SEEK_SET)  # Go to the File index for the piece
                os.write(self.fd, piece_data[:(fracture-piece_abs_location)])  # Write first fragment of the piece from beg upto length of first fragment (fracture point - piece_abs_location)
                ####SECOND FRAGMENT####
                self.fd = os.open(os.path.join(self.file_path, next_file), os.O_RDWR | os.O_CREAT)
                os.lseek(self.fd, 0, os.SEEK_SET)  # Go to beginning of the next file
                os.write(self.fd, piece_data[(fracture-piece_abs_location):])  # Write second fragment of the piece from fracture point to end
                piece_instance.flush()  # Remove from RAM after writing to disk
