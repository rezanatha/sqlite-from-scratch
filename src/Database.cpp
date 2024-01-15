#include "Database.h"

namespace Database {
    std::ifstream *db;

    std::vector<RowField> read_row (size_t offset) {
        std::vector<RowField> fields;
        size_t field_id_offset = offset;
        size_t row_header_size = Decode::read_varint(db, offset, field_id_offset);
        //printf("row header size: %zu \n", row_header_size);
        //printf("offset: %zu, field_id_offset: %zu \n", offset, field_id_offset);
        size_t start = field_id_offset;
        size_t end = start + row_header_size - 1;
        while (start < end) {
            uint32_t serial_type = Decode::read_varint(db, start, start);
            //printf("current offset: %zu, serial type: %d \n", start, serial_type);

            if (serial_type == 0) {
                RowField field;
                field.field_type = SERIAL_NULL;
                field.field_size = 0;
                fields.push_back(field);
            } else if (serial_type == 1) {
                RowField field;
                field.field_type = SERIAL_8_BIT_INTEGER;
                field.field_size = 1;
                fields.push_back(field);
            } else if (serial_type == 2) { 
                RowField field;
                field.field_type = SERIAL_16_BIT_INTEGER;
                field.field_size = 2;
                fields.push_back(field);
            } else if (serial_type == 3) { 
                RowField field;
                field.field_type = SERIAL_24_BIT_INTEGER;
                field.field_size = 3;
                fields.push_back(field);
            } else if (serial_type >= 13 && serial_type & 1) {
                RowField field;
                field.field_type = SERIAL_BLOB_ODD;
                field.field_size = (serial_type - 13) >> 1;
                fields.push_back(field);
            } 

        }
        //std::cout << "field size: " << fields.size() << std::endl;
        for (auto it = fields.begin(); it != fields.end(); ++it) {
            if (it->field_type == SERIAL_NULL) {
                //(no need to read db since field_size = 0)
                it->field_value = nullptr;
            }
            else if (it->field_type == SERIAL_8_BIT_INTEGER) {
                char buf[1] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                start += it->field_size;
                int8_t *val = new int8_t(buf[0]);
                it->field_value = static_cast<void*>(val);

            } else if (it->field_type == SERIAL_16_BIT_INTEGER) {
                char buf[2] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                start += it->field_size;
                int16_t *val = new int16_t(static_cast<unsigned char>(buf[0]));
                it->field_value = static_cast<void*>(val);

            } else if (it->field_type == SERIAL_24_BIT_INTEGER) {
                char buf[3] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                start += it->field_size;
                uint32_t *val = new uint32_t(static_cast<unsigned char>(buf[0]));
                it->field_value = static_cast<void*>(val);

            } else if (it->field_type == SERIAL_BLOB_ODD) {
                char buff[1024] = {};
                db->seekg(start, std::ios::beg);
                db->read(buff, it->field_size);
                start += it->field_size;
                std::string *s = new std::string(buff);
                it->field_value = static_cast<void*>(s);
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
        db->seekg(Database::HeaderSize::DATABASE + 3, std::ios::beg);
        char buf[2];
        Database::db->read(buf, 2);
        return Decode::to_uint16_t(buf);
    }

    std::vector<TableLeafCell> read_master_table () {
        // Master table is a leaf b-tree that resides just after database header. 
        // Cell offsets are located after master table's header.
        uint16_t cell_count = master_header_cell_count();
        int cell_offset = Database::HeaderSize::DATABASE + Database::HeaderSize::LEAF; 
        //std::cout << "offset: " << cell_offset << std::endl;
        std::vector<size_t> cell_offsets;
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

        std::vector<TableLeafCell> master_table;
        for (auto &offset: cell_offsets) {
            size_t row_id_offset = offset, payload_offset = offset;
            TableLeafCell cell = {
                .payload_size = static_cast<size_t>(Decode::read_varint(Database::db, offset, row_id_offset)),
                .rowid =  static_cast<uint32_t>(Decode::read_varint(Database::db, row_id_offset, payload_offset)),
                .field = read_row(payload_offset),
            };
            // Row row = {
            //     .row_size = Decode::read_varint(Database::db, offset, row_id_offset),
            //     .row_id = Decode::read_varint(Database::db, row_id_offset, payload_offset),
            //     .field = read_row(payload_offset),
            // };
            master_table.push_back(cell);
        }
        return master_table;
    }

    PageHeader read_page_header (TableLeafCell* master_table_row, const uint32_t page_size) {
        //get page offset
        uint16_t root_page = *static_cast<uint16_t*>(master_table_row->field[3].field_value);
        const size_t page_offset = (root_page - 1) * page_size;

        //std::cout << "root page: " << root_page << " page offset " << page_offset << std::endl;

        //read page types
        Database::db->seekg(page_offset, std::ios::beg);
        char buf[1] = {};
        Database::db->read(buf, 1);
        uint16_t page_type = buf[0];

        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buff[2] = {};
        Database::db->read(buff, 2);
        uint16_t cell_count = Decode::to_uint16_t(buff);

        //std::cout << "cell count: " << cell_count << std::endl;

        return {
            .name = *static_cast<std::string*>(master_table_row->field[2].field_value),
            .definition = *static_cast<std::string*>(master_table_row->field[4].field_value),
            .page_offset = page_offset,
            .page_type = page_type,
            .cell_count = cell_count,
        };
   }
}    
