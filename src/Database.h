#ifndef DATABASE_H
#define DATABASE_H

#include <iostream>
#include <vector>
#include "Decode.h"

namespace Database {

    extern std::ifstream *db;

    enum HeaderSize {
        DATABASE = 100,
        LEAF = 8,
        INTERIOR = 12
    };

    enum PageType {
        INTERIOR_INDEX = 2,
        INTERIOR_TABLE = 5,
        LEAF_INDEX = 10,
        LEAF_TABLE = 13
    };

    enum SerialType {
        SERIAL_NULL = 0,
        SERIAL_8_BIT_INTEGER = 1,
        INTEGER_0 = 8,
        INTEGER_1 = 9,
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

    struct TableLeafCell {
        size_t payload_size;
        uint32_t rowid;
        std::vector<RowField> field;
    };

    struct TableInteriorCell {
        uint32_t left_child_pointer;
        uint32_t rowid;
    };

    std::vector<RowField> read_row (size_t offset);
    std::vector<RowField> read_row_debug (size_t offset);
    uint32_t db_header_page_size();
    uint32_t db_header_num_of_pages();
    uint32_t db_header_encoding();

    uint16_t master_header_cell_count();
    std::vector<TableLeafCell> read_master_table();
}

#endif