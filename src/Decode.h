#ifndef DECODE_H
#define DECODE_H

#include <iostream>
#include <fstream>
#include <cstring>
#include <arpa/inet.h>

namespace Decode {
    uint16_t to_uint16_t (const char *buffer);
    uint32_t to_uint32_t (const char *buffer);
    int read_varint (std::ifstream *db, uint16_t starting_offset, uint16_t &new_offset);
}
 
#endif