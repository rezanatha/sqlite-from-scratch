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
        INDEX_INTERIOR = 2,
        TABLE_INTERIOR = 5,
        INDEX_LEAF = 10,
        TABLE_LEAF = 13
    };

    enum SerialType {
        SERIAL_NULL = 0,
        SERIAL_8_BIT_INTEGER = 1,
        SERIAL_16_BIT_INTEGER = 2,
        SERIAL_24_BIT_INTEGER = 3,
        SERIAL_32_BIT_INTEGER = 4,
        SERIAL_48_BIT_INTEGER = 5,
        SERIAL_64_BIT_INTEGER = 6,
        SERIAL_64_BIT_FLOAT = 7,
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
        SerialType field_type;     // RECORD HEADER
        int field_size;            // TRANSLATED FROM RECORD HEADER
        void* field_value;         // BODY
    };

    struct PageHeader {
        std::string name;
        std::string type;
        std::string definition;
        size_t page_offset;
        uint16_t page_type;
        size_t cell_count;
    };

    struct TableLeafCell {
        size_t offset;
        size_t payload_size;
        int32_t rowid;
        std::vector<RowField> field;
    };

    struct TableLeafPage {
        /* header */
        uint8_t page_type;              // offset 0
        uint16_t cell_count;            // offset 3
        /* cell array */
        std::vector<TableLeafCell> cells;
    };

    struct TableInteriorCell {
        uint32_t left_child_pointer;
        uint32_t rowid;
    };

    struct TableInteriorPage {
        /* header */
        uint8_t page_type;              // offset 0
        uint16_t cell_count;            // offset 3
        uint32_t rightmost_pointer;     // offset 8
        /* cell array */
        std::vector<TableInteriorCell> cells;
    };

    struct IndexInteriorCell {
        uint32_t left_child_pointer;     
        size_t payload_size;             // total header size (the size itself + bytes of column/field types)
        std::vector<RowField> field;     // contains column type (and thus, size) & column value
        uint32_t first_overflow_page;
    };

    struct IndexInteriorPage {
        /* header */
        uint8_t page_type;              // offset 0
        uint16_t cell_count;            // offset 3
        uint32_t rightmost_pointer;     // offset 8
        /* cell array */
        std::vector<IndexInteriorCell> cells;
    };

    struct IndexLeafCell {
        size_t payload_size;   
        std::vector<RowField> field;
        uint32_t first_overflow_page;
    };

    struct IndexLeafPage {
        /* metadata */
        
        /* header */
        uint8_t page_type;              // offset 0
        uint16_t cell_count;            // offset 3

        /* cell array */
        std::vector<IndexLeafCell> cells;
    };

    std::vector<RowField> read_row (size_t offset);
    //std::vector<RowField> read_row_debug (size_t offset);
    uint32_t db_header_page_size();
    uint32_t db_header_num_of_pages();
    uint32_t db_header_encoding();

    uint16_t master_header_cell_count();
    std::vector<TableLeafCell> read_master_table();
    PageHeader read_page_header (TableLeafCell &master_table_row, const uint32_t page_size);

    uint8_t read_1_byte_from_db(std::ifstream *db, size_t offset);
    uint16_t read_2_bytes_from_db(std::ifstream *db, size_t offset);
    uint32_t read_4_bytes_from_db(std::ifstream *db, size_t offset);

}

#endif