#include "Database.h"

namespace Database {
    std::ifstream *db;

    std::vector<RowField> read_row (size_t offset) {
        int start_offset = offset;
        std::vector<RowField> fields;
        size_t field_id_offset = offset;
        size_t row_header_size = Decode::read_varint_new(db, field_id_offset);
        //printf("row header size: %zu \n", row_header_size);

        size_t start = field_id_offset;
        size_t end = start + row_header_size - 1;
        while (start < end) {
            uint32_t serial_type = Decode::read_varint_new(db, start);
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
                char buf[SERIAL_16_BIT_INTEGER] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                it->field_value = malloc(SERIAL_16_BIT_INTEGER);
                memcpy(it->field_value, buf, SERIAL_16_BIT_INTEGER);
                start += it->field_size;

            } else if (it->field_type == SERIAL_24_BIT_INTEGER) {
                char buf[SERIAL_24_BIT_INTEGER] = {};
                db->seekg(start, std::ios::beg);
                db->read(buf, it->field_size);
                it->field_value = malloc(SERIAL_24_BIT_INTEGER);
                memcpy(it->field_value, buf, SERIAL_24_BIT_INTEGER);

                start += it->field_size;
                //it->field_value = static_cast<void*>(buf);

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
        size_t cell_offset = Database::HeaderSize::DATABASE + Database::HeaderSize::LEAF; 
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            cell_offsets.push_back(read_2_bytes_from_db(db, cell_offset));
            cell_offset += 2;
        }

        std::vector<TableLeafCell> master_table;
        for (auto &offset: cell_offsets) {
            size_t var_offset = offset;

            TableLeafCell cell;
            cell.payload_size = static_cast<size_t>(Decode::read_varint_new(Database::db, var_offset));
            cell.rowid =  static_cast<int32_t>(Decode::read_varint_new(Database::db, var_offset));
            cell.field = read_row(var_offset);
            
            master_table.push_back(cell);
        }
        return master_table;
    }

    PageHeader read_page_header (TableLeafCell &master_table_row, const uint32_t page_size) {
        //get page offset
        size_t root_page;
        if (master_table_row.field[3].field_type == 3) {
            root_page = Decode::deserialize_24_bit_to_signed(static_cast<char*>(master_table_row.field[3].field_value));
        } else {
            root_page = *static_cast<uint8_t*>(master_table_row.field[3].field_value);
        }
        //uint32_t root_page = *static_cast<uint16_t*>(master_table_row.field[3].field_value);
        const size_t page_offset = (root_page - 1) * page_size;

        //std::cout << "root page: " << root_page << " page offset " << page_offset << std::endl;

        //read page types
        uint16_t page_type = read_1_byte_from_db(Database::db, page_offset);

        //read cell count
        uint16_t cell_count = read_2_bytes_from_db(Database::db, page_offset + 3);

        //std::cout << "name: " << *static_cast<std::string*>(master_table_row.field[1].field_value) << " " << page_type << std::endl;

        PageHeader header;
        header.name = *static_cast<std::string*>(master_table_row.field[2].field_value);
        header.type = *static_cast<std::string*>(master_table_row.field[0].field_value);
        header.definition = *static_cast<std::string*>(master_table_row.field[4].field_value);
        header.page_offset = page_offset;
        header.page_type = page_type;
        header.cell_count = cell_count;
        return header;

   }
   uint8_t read_1_byte_from_db(std::ifstream *db, size_t offset) {
        db->seekg(offset, std::ios::beg);
        char buf[1] = {};
        db->read(buf, 1);

        return buf[0];    
   }
   uint16_t read_2_bytes_from_db(std::ifstream *db, size_t offset) {
        db->seekg(offset, std::ios::beg);
        char buf[2] = {};
        db->read(buf, 2);
        
        return Decode::to_uint16_t(buf);    
   }
   uint32_t read_4_bytes_from_db(std::ifstream *db, size_t offset) {
        db->seekg(offset, std::ios::beg);
        char buf[4] = {};
        db->read(buf, 4);
        
        return Decode::to_uint32_t(buf);    
   }
}    
