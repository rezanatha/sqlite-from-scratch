#include "Database.h"

namespace Database {
    std::ifstream *db;

    std::vector<RowField> read_row_debug (uint16_t offset) {
        std::vector<RowField> fields;
        uint16_t field_id_offset = offset;
        int row_header_size = Decode::read_varint(db, offset, field_id_offset);
        //std::cout << "starting payload offset (header): " << offset << std::endl;
        // int row_header_size = Decode::read_varint_new(db, offset);
        // field_id_offset = offset;
        //std::cout << "starting column varint offset: " << field_id_offset << std::endl;

        printf("row header size: %d \n", row_header_size);
        //printf("field_id_offset: %u \n", field_id_offset);
        uint16_t start = field_id_offset;
        int end = start + row_header_size - 1;
        printf("(start, end) = %d %d \n", start, end);
        int counter = 0;
        while (start < end) {
            counter++;
            int32_t serial_type = Decode::read_varint(db, start, start);
            printf("serial type: %d \n", serial_type);
            if (serial_type >= 13 && serial_type & 1) {
                RowField field;
                field.field_type = SERIAL_BLOB_ODD;
                field.field_size = (serial_type - 13) >> 1;
                fields.push_back(field);
            } else if (serial_type >= 12 && !(serial_type & 1)) {
                RowField field;
                field.field_type = SERIAL_BLOB_EVEN;
                field.field_size = (serial_type - 12) >> 1;
                fields.push_back(field);
            } else if (serial_type == 1) {
                RowField field;
                field.field_type = SERIAL_8_BIT_INTEGER;
                field.field_size = 1;
                fields.push_back(field);
            } else if (serial_type == 9) {
                RowField field;
                field.field_type = INTEGER_1;
                field.field_size = 0;
                fields.push_back(field);              
            }
        }

        std::cout << "done " << counter << " times, pushed " << fields.size() << " times" << " start offset: " << start << std::endl;
        for (auto it = fields.begin(); it != fields.end(); ++it) {
            std::cout << "field type: " << it->field_type << std:: endl;
            if (it->field_type == SERIAL_BLOB_ODD) {
                std::cout << "start: " << start << std::endl;
                char buff[1024] = {};
                db->seekg(start, std::ios::beg);
                db->read(buff, it->field_size);
                start += it->field_size;
                std::string *s = new std::string(buff);
                std::cout << "buff: " << *s << std::endl;
                it->field_value = static_cast<void*>(s);

            } else if (it->field_type == SERIAL_BLOB_EVEN) {
                std::cout << "start: " << start << std::endl;
                char buff[1024] = {};
                db->seekg(start, std::ios::beg);
                db->read(buff, it->field_size);
                start += it->field_size;
                std::string *s = new std::string(buff);
                std::cout << "buff: " << *s << std::endl;
                it->field_value = static_cast<void*>(s);

            } else if (it->field_type == SERIAL_8_BIT_INTEGER) {
                char buf[1] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                start += it->field_size;
                uint16_t *val = new uint16_t(static_cast<unsigned char>(buf[0]));
                it->field_value = static_cast<void*>(val);
            }
        }
        std::cout << "done" << std::endl;
        return fields;
    }

    std::vector<RowField> read_row (uint16_t offset) {
        std::vector<RowField> fields;
        uint16_t field_id_offset = offset;
        int row_header_size = Decode::read_varint(db, offset, field_id_offset);
        //printf("row header size: %d \n", row_header_size);
        //printf("field_id_offset: %u \n", field_id_offset);
        uint16_t start = field_id_offset;
        int end = start + row_header_size - 1;
        while (start < end) {
            int32_t serial_type = Decode::read_varint(db, start, start);
            //printf("serial type: %d \n", serial_type);
            if (serial_type >= 13 && serial_type & 1) {
                RowField field;
                field.field_type = SERIAL_BLOB_ODD;
                field.field_size = (serial_type - 13) >> 1;
                fields.push_back(field);
            } else if (serial_type == 1) {
                RowField field;
                field.field_type = SERIAL_8_BIT_INTEGER;
                field.field_size = 1;
                fields.push_back(field);
            }
        }
        for (auto it = fields.begin(); it != fields.end(); ++it) {
            if (it->field_type == SERIAL_BLOB_ODD) {
                char buff[1024] = {};
                db->seekg(start, std::ios::beg);
                db->read(buff, it->field_size);
                start += it->field_size;
                std::string *s = new std::string(buff);
                it->field_value = static_cast<void*>(s);
            } else if (it->field_type == SERIAL_8_BIT_INTEGER) {
                char buf[1] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                start += it->field_size;
                uint16_t *val = new uint16_t(static_cast<unsigned char>(buf[0]));
                it->field_value = static_cast<void*>(val);
            }
        }
        return fields;
    }

    uint32_t db_header_page_size() {
        db->seekg(16, std::ios::beg);
        char page_size_buffer[2];
        db->read(page_size_buffer, 2);
        uint32_t page_size = Decode::to_uint16_t(page_size_buffer);

        //Read page size: Exception on page_size_buffer = 0x00 0x01, it represents value 65536
        if(page_size == 1) { 
            return 65536;
        } else {
            return page_size;
        }
    }

    uint32_t db_header_num_of_pages() {
        db->seekg(28, std::ios::beg);
        char buf[4];
        db->read(buf, 4);
        return Decode::to_uint32_t(buf);
    }

    uint32_t db_header_encoding () {
        db->seekg(56, std::ios::beg);
        char buf[4] = {};
        db->read(buf, 4);
        return Decode::to_uint32_t(buf);
    }

    uint16_t master_header_cell_count() {
        db->seekg(DB_HEADER_SIZE + 3, std::ios::beg);
        char buf[2];
        Database::db->read(buf, 2);
        return Decode::to_uint16_t(buf);
    }

    std::vector<Cell> read_master_table () {
        // Master table is a leaf b-tree that resides just after database header. 
        // Cell offsets are located after master table's header.
        uint16_t cell_count = master_header_cell_count();
        int cell_offset = DB_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE; 
        //std::cout << "offset: " << cell_offset << std::endl;
        std::vector<uint16_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            db->seekg(cell_offset, std::ios::beg);
            char buf[2] = {};
            db->read(buf, 2);
            cell_offsets.push_back(Decode::to_uint16_t(buf));
            cell_offset += 2;
        }

        // for(auto &a: cell_offsets) {
        //     std::cout << a << std::endl;
        // }

        std::vector<Cell> master_table;
        for (auto &offset: cell_offsets) {
            uint16_t row_id_offset = offset, payload_offset = offset;
            Cell cell;
            cell.payload_size = Decode::read_varint(Database::db, offset, row_id_offset);
            cell.rowid = Decode::read_varint(Database::db, row_id_offset, payload_offset);
            cell.field = read_row(payload_offset);
            // Row row = {
            //     .row_size = Decode::read_varint(Database::db, offset, row_id_offset),
            //     .row_id = Decode::read_varint(Database::db, row_id_offset, payload_offset),
            //     .field = read_row(payload_offset),
            // };
            master_table.push_back(cell);
        }
        return master_table;
    }
}    
