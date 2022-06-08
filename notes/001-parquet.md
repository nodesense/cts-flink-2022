```
Country

United Kingdom
United Kingdom
United Kingdom
United Kingdom
United Kingdom
United Kingdom
United Kingdom
France
Canada
United Kingdom
France
Canada
United Kingdom
France
Canada
United Kingdom
France
Canada

205 - chars, each char is 2 bytes = 400 bytes if data stored as is

United Kingdom - 1
France - 2
CAnada - 3

Country column encoded with Dict Encoding

1
1
1
1
1
1
1
2
3
1
2
3
1
2
3
1
2
3

35 - chars/bytes/int 35 x 4 bytes = 140 bytes

RLE encoding

(United Kingdom, 7) (France, 1) (Canada, 1)..

Further after encoding, data is compretedd in two formats gzip [slow], snappy [fastest decompression]
```