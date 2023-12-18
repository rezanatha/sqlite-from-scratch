#ifndef DATABASE_H
#define DATABASE_H

#include <iostream>
#include <vector>
#include "Decode.h"

namespace Database {

    extern std::ifstream *db;

    constexpr int DB_HEADER_SIZE = 100;
    constexpr int LEAF_PAGE_HEADER_SIZE = 8;
    constexpr int INTERIOR_PAGE_HEADER_SIZE = 12;

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
    size_t row_size;
    uint32_t row_id;
    std::vector<RowField> field;
    };

    std::vector<RowField> read_row (uint16_t offset);
    std::vector<RowField> read_row_debug (uint16_t offset);
    uint32_t db_header_page_size();
    uint32_t db_header_num_of_pages();
    uint32_t db_header_encoding();

    uint16_t master_header_cell_count();
    std::vector<Row> read_master_table();
}

#endif