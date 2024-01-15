#include "Decode.h"

namespace Decode {
    uint16_t to_uint16_t (const char *buffer) {
        /* read EXACTLY 2 bytes in big endian */
        uint16_t result;
        memcpy(&result, buffer, sizeof(uint16_t));
        return ntohs(result);
    }

    uint32_t deserialize_24_bit_to_unsigned (const char *buffer) {
        /* read EXACTLY 3 bytes in big endian, return uint32_t*/
        uint32_t result = 0;
        result |= static_cast<unsigned char>(buffer[0]) << 16;
        result |= static_cast<unsigned char>(buffer[1]) << 8;
        result |= static_cast<unsigned char>(buffer[2]);

        std::cout << std::bitset<8>(buffer[0]) 
        << ' ' << std::bitset<8>(buffer[1]) 
        << ' ' << std::bitset<8>(buffer[2]) 
        << std::endl;

        return result;
    }
    uint32_t to_uint32_t (const char *buffer) {
        /* read EXACTLY 4 bytes in big endian */
        uint32_t result;
        memcpy(&result, buffer, sizeof(uint32_t));
        return ntohl(result);
    }
    uint64_t read_varint (std::ifstream *db, size_t starting_offset, size_t &new_offset) {
        /*
        Read bytes from the varint_start (equal to offset)
        If bit at varint_start equals to 0, then it is the last byte
        If bit at varint_start equals to 1, then more bytes follow
        */
        size_t start = starting_offset;
        uint64_t varint_value = 0;
        char buf[1];
        while (true) {
            db->seekg(start, std::ios::beg);
            db->read(buf, 1); 
            varint_value = varint_value | buf[0]; //concat our bit
            start++;
            if (!(buf[0] & 0x80))  { //MSB is 0
                break;
            }
            varint_value = (varint_value & 0x7F) << 7;
        }  
        new_offset = start;
    return varint_value;
    }

    uint64_t read_varint_new (std::ifstream *db, uint16_t &offset) {
        db->seekg(offset, std::ios::beg);
        uint64_t result = 0;
        int shift = 0;
        uint8_t byte;
    
        do {
            if (!db->read(reinterpret_cast<char*>(&byte), sizeof(byte))) {
                // Handle error or end of file
                throw std::runtime_error("Error reading varint from the stream");
            }
    
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;
            shift += 7;
            offset++;
        } while (byte & 0x80);

    
    return result;
    }
}






