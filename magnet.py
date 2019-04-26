from pprint import pprint


class Magnet:
    def __init__(self, magnet_url):
        if magnet_url != '' and magnet_url[:6] == 'magnet':
            print('Valid Magnet URI')
            self._metaData = self._parse(magnet_url)

        self._keys = [elem[0] for elem in self._metaData]

        self._name = [elem for elem in self._metaData if elem[0] == 'dn'][0]

        self._total_length = [elem for elem in self._metaData if elem[0] == 'xl'][0]

        self._isPrivate = False  # Magnets don't support private trackers for now

        self._info_hash = [elem for elem in self._metaData if elem[0] == 'xt'][0]

        if 'xs' in self._keys:
            self._source = [elem for elem in self._metaData if elem[0] == 'xs'][0]

        if 'kt' in self._keys:
            self._keywords = [elem for elem in self._metaData if elem[0] == 'kt'][0]

        if 'mt' in self._keys:
            self._manifest = [elem for elem in self._metaData if elem[0] == 'mt'][0]

        if 'tr' in self._keys:
            self._trackers = [elem for elem in self._metaData if elem[0] == 'tr']

        self._pieces = ''

        self._piece_length = ''

    @staticmethod
    def _parse(url):
        params = [tuple(elem.split('=')) for elem in url.replace('magnet:?', '').split('&')]
        pprint(params)
        return params


if __name__ == '__main__':
    url = "magnet:?xt=urn:ed2k:31D6CFE0D16AE931B73C59D7E0C089C0&xl=0&dn=zero_len.fil&xt=urn:bitprint:3I42H3S6NNFQ2M" \
          "SVX7XZKYAYSCX5QBYJ.LWPNACQDBZRYXW3VHJVCJ64QBZNGHOHHHZWCLNQ&xt=urn:md5:D41D8CD98F00B204E9800998ECF8427E"

    m = Magnet(url)
