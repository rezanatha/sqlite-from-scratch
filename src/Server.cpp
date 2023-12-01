#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>

constexpr int DB_HEADER_SIZE = 100;
constexpr int LEAF_PAGE_HEADER_SIZE = 8;
constexpr int INTERIOR_PAGE_HEADER_SIZE = 12;

std::ifstream database_file;

struct RowField {
    int field_type;
    int field_size;
    void* field_value;
};

unsigned short parse_short (const char *buffer) {
    /* read 2 bytes in big endian */
    return ((static_cast<unsigned char>(buffer[0]) << 8) | static_cast<unsigned char>(buffer[1]));
}

int read_varint (unsigned short starting_offset, unsigned short &new_offset) {
    /*
    Read bytes from the varint_start (equal to offset)
    If bit at varint_start equals to 0, then it is the last byte
    If bit at varint_start equals to 1, then more bytes follow
    */
    unsigned short start = starting_offset;
    int varint_value = 0;
    char buf[1];
    while (true) {
        database_file.seekg(start);
        database_file.read(buf, 1); 
        varint_value = varint_value | buf[0]; //concat our bit
        start++;
        if (!(buf[0] & 0x80))  { //MSB is 0
            break;
        }
        varint_value = (varint_value & 0x7F) << 8;
    }  
    new_offset = start;
    return varint_value;
}

std::vector<RowField> read_row (unsigned short offset) {
    /*
    Payload structure:
    Row header size
    Field ID
    Value
    */
    std::vector<RowField> fields;
    unsigned short field_id_offset = offset;
    int row_header_size = read_varint(offset, field_id_offset);
    unsigned short start = field_id_offset;
    int end = start + row_header_size - 1;
    while (start < end) {
        int field_id = read_varint(start, start);
        if (field_id >= 13 && field_id & 1) {
            RowField field;
            field.field_size = (field_id - 13) >> 1;
            fields.push_back(field);
        }
    }
    for (auto it = fields.begin(); it != fields.end(); ++it) {
        char buf[1024] = {};
        database_file.seekg(start);
        database_file.read(buf, it->field_size);
        start += it->field_size;
        std::string *s = new std::string(buf);
        it->field_value = static_cast<void*>(s);
    }

    return fields;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];

    database_file.open(database_file_path, std::ios::binary);

    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }

    //Read page size
    database_file.seekg(16, std::ios::beg);
    char page_size_buffer[2];
    database_file.read(page_size_buffer, 2);
    unsigned int page_size = parse_short(page_size_buffer);

    //Read page size: Exception on page_size_buffer = 0x00 0x01, it represents value 65536
    if(page_size == 1) { 
        page_size = 65536;
    }

    //Read cell count
    database_file.seekg(DB_HEADER_SIZE + 3, std::ios::beg);
    char cell_count_buffer[2];
    database_file.read(cell_count_buffer, 2);
    unsigned short cell_count = parse_short(cell_count_buffer);

    if (command == ".dbinfo") {
        printf("database page size: %u \n", page_size);
        printf("number of tables: %u \n", cell_count); 

    } else if (command == ".tables") {
        // Master table is a leaf b-tree that resides just after database header. 
        // Cell offsets are located after master table's header.
        int cell_offset = DB_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE; 
        
        std::vector<unsigned short> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            database_file.seekg(cell_offset, std::ios::beg);
            char buf[2];
            database_file.read(buf, 2);
            cell_offsets.push_back(parse_short(buf));
            cell_offset += 2;
        }
        for (auto &offset: cell_offsets) {
            /*
            Row cell structure:
            Row Size
            Row ID
            Payload
            */
            unsigned short row_id_offset = offset, payload_offset = offset;
            int row_size = read_varint(offset, row_id_offset);
            int row_id = read_varint(row_id_offset, payload_offset);
            std::vector<RowField> payload = read_row(payload_offset);
            //printf("cell pointer %u, row size %d, row_id %d\n", offset, row_size, row_id);
            std::cout << *static_cast<std::string*>(payload[1].field_value) << " ";
        }

    }

    return 0;
}
