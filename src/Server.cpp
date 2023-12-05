#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <memory>
#include <sstream>

constexpr int DB_HEADER_SIZE = 100;
constexpr int LEAF_PAGE_HEADER_SIZE = 8;
constexpr int INTERIOR_PAGE_HEADER_SIZE = 12;

std::ifstream database_file;

enum PageType {
    INTERIOR_INDEX = 2,
    INTERIOR_TABLE = 5,
    LEAF_INDEX = 10,
    LEAF_TABLE = 13
};

enum SerialType {
    SERIAL_NULL = 0,
    SERIAL_8_BIT_INTEGER = 1,
    SERIAL_BLOB_EVEN = 12,
    SERIAL_BLOB_ODD = 13
};

enum Encoding {
    UTF8 = 1,
    UTF16LE = 2,
    UTF16BE = 3
};

struct RowField {
    SerialType field_type;
    int field_size;
    void* field_value;
};

struct Row {
    unsigned int row_id;
    size_t row_size;
    std::vector<RowField> field;
};

std::vector<std::string> split_string (const std::string &str, const char delimiter) {
    std::vector<std::string> output;
    std::stringstream ss;
    ss << str;
    std::string token;

    while (std::getline(ss, token, delimiter) ) {
        output.push_back(token);
    }
    return output;
}

void parse_sql_command (const std::string &command) {

}

unsigned short parse_short (const char *buffer) {
    /* read 2 bytes in big endian */
    return ((static_cast<unsigned char>(buffer[0]) << 8) | static_cast<unsigned char>(buffer[1]));
}

unsigned int parse_int (const char *buffer) {
    /* read 4 bytes in big endian */
    return ((static_cast<unsigned char>(buffer[0]) << 16) 
            | static_cast<unsigned char>(buffer[1])
            | static_cast<unsigned char>(buffer[2])
            | static_cast<unsigned char>(buffer[3])
            );
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
        int serial_type = read_varint(start, start);
        if (serial_type >= 13 && serial_type & 1) {
            RowField field;
            field.field_type = SERIAL_BLOB_ODD;
            field.field_size = (serial_type - 13) >> 1;
            fields.push_back(field);
        } else if (serial_type == 1) {
            RowField field;
            //printf("serial type: %d \n", serial_type);
            field.field_type = SERIAL_8_BIT_INTEGER;
            field.field_size = 1;
            fields.push_back(field);
        }

    }
    for (auto it = fields.begin(); it != fields.end(); ++it) {
        if (it->field_type == SERIAL_BLOB_ODD) {
            char buf[1024] = {};
            database_file.seekg(start, std::ios::beg);
            database_file.read(buf, it->field_size);
            start += it->field_size;
            std::string *s = new std::string(buf);
            it->field_value = static_cast<void*>(s);
        } else if (it->field_type == SERIAL_8_BIT_INTEGER) {
            char buf[1] = {};
            database_file.seekg(start, std::ios::beg);
            database_file.read(buf, it->field_size);
            start += it->field_size;
            unsigned short *val = new unsigned short(static_cast<unsigned char>(buf[0]));
            //printf("rp: %u \n", *val);
            it->field_value = static_cast<void*>(val);
        }
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

    //Read number of page size
    database_file.seekg(28, std::ios::beg);
    char buf[4];
    database_file.read(buf, 4);
    unsigned int num_page = parse_int(buf);

    //Read cell count
    database_file.seekg(DB_HEADER_SIZE + 3, std::ios::beg);
    char cell_count_buffer[2];
    database_file.read(cell_count_buffer, 2);
    unsigned short cell_count = parse_short(cell_count_buffer);

    //Read encoding
    database_file.seekg(56, std::ios::beg);
    char enc_buf[4] = {};
    database_file.read(enc_buf, 4);
    unsigned int encoding = parse_int(enc_buf);

    // Process master table
    // Master table is a leaf b-tree that resides just after database header. 
    // Cell offsets are located after master table's header.
    int cell_offset = DB_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE; 
    
    std::vector<unsigned short> cell_offsets;
    std::vector<Row> master_table;
    std::map<std::string, unsigned short> root_page_map;
    for (int i = 0; i < cell_count; ++i) {
        database_file.seekg(cell_offset, std::ios::beg);
        char buf[2] = {};
        database_file.read(buf, 2);
        cell_offsets.push_back(parse_short(buf));
        cell_offset += 2;
    }
    for (auto &offset: cell_offsets) {
        Row row;
        unsigned short row_id_offset = offset, payload_offset = offset;
        row.row_size = read_varint(offset, row_id_offset);
        row.row_id = read_varint(row_id_offset, payload_offset);
        row.field = read_row(payload_offset);
        master_table.push_back(row);
        std::string table_name = *static_cast<std::string*>(row.field[2].field_value);
        unsigned short root_page_num = *static_cast<unsigned short*>(row.field[3].field_value);
        root_page_map[table_name] = root_page_num; 
    }
    //Process command
    if (command == ".dbinfo") {
        printf("database page size: %u \n", page_size);
        //printf("number of pages: %u \n", num_page);
        printf("number of tables: %u \n", cell_count); 
        //printf("encoding: %u \n", encoding); 

    } else if (command == ".tables") {
        for (auto &r: master_table) {
            //printf("row size %zu, row_id %d ", r.row_size, r.row_id); 
            std::cout << *static_cast<std::string*>(r.field[2].field_value) << std::endl;
        }

    } else if (std::vector<std::string> c = split_string(command, ' '); c[1] == "COUNT(*)") {
        std::string table = c.back();
        unsigned short root_page = root_page_map[table];
        database_file.seekg((root_page - 1) * page_size + 3, std::ios::beg);
        buf[2] = {};
        database_file.read(buf, 2);
        unsigned short cell_count = parse_short(buf);
        printf("%u\n", cell_count);
    }

    /*
        parse sql query
        find where the command and the table lie
        find the table, we got to the root page
    */
    /* Reading database page
    1. Check page type (offset 0), to get the header size
    2. If command == COUNT(*), then take offset 3 to get number of cells
    3. To access the rows, offset is + HEADER_SIZE
    */



    return 0;
}
