#ifndef DECODE_H
#define DECODE_H

#include <iostream>
#include <fstream>
#include <cstring>
#include <arpa/inet.h>

namespace Decode {
    uint16_t to_uint16_t (const char *buffer);
    uint32_t deserialize_24_bit_to_unsigned (const char *buffer);
    int32_t deserialize_24_bit_to_signed (const char *buffer);
    uint32_t to_uint32_t (const char *buffer);
    uint64_t read_varint (std::ifstream *db, size_t starting_offset, size_t &new_offset);
    uint64_t read_varint_new (std::ifstream *db, size_t &offset);
}
 
#endif