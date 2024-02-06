#include "Query.h"
#include "Database.h"
#include "Decode.h"

namespace Query {
/* FUNCTIONS FOR PROCESSING PAGES & CELLS */
    Database::TableInteriorPage read_table_interior_page (size_t page_offset)  {
        Database::TableInteriorPage page;
        //read cell count
        uint16_t cell_count = Database::read_2_bytes_from_db(Database::db, page_offset + 3);
        page.cell_count = cell_count;

        //read rightmost pointer
        uint32_t rightmost_pointer = Database::read_4_bytes_from_db(Database::db, page_offset + 8);
        page.rightmost_pointer = rightmost_pointer;

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::INTERIOR;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            cell_offset += 2;
        }

        //read cells for interior table page
        //if our page type is interior table, then a cell contains of:
        //4-byte left child pointer
        //varint row ID
        std::vector<Database::TableInteriorCell> cells;
        for (auto &offset: cell_offsets) {
            uint32_t left_child_pointer = Database::read_4_bytes_from_db(Database::db, offset);
            size_t rowid_offset = offset + 4;
            uint32_t rowid = Decode::read_varint_new(Database::db, rowid_offset);
            Database::TableInteriorCell cell = {.left_child_pointer = left_child_pointer, .rowid = rowid};
            cells.push_back(cell);

            //std::cout << "lcp: " << left_child_pointer << " rowid: " << rowid << std::endl;
        }
        page.cells = cells;

        return page;

    }
    
    std::vector<Database::TableLeafCell> read_table_leaf_cell (size_t page_offset) {
        //read cell count
        uint16_t cell_count = Database::read_2_bytes_from_db(Database::db, page_offset + 3);

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::LEAF;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            cell_offset += 2;
        }

        //read cells
        //page type is a leaf table, a cell contains of:
        //varint payload size
        //varint row id
        //(payload size) payload
        std::vector<Database::TableLeafCell> table;
        for (auto &offset: cell_offsets) {
            size_t row_id_offset = offset, payload_offset = offset;
            Database::TableLeafCell cell;
            cell.offset = offset;
            cell.payload_size = Decode::read_varint_new(Database::db, offset);

            cell.rowid = Decode::read_varint_new(Database::db, offset);
            
            cell.field = Database::read_row(offset);
            table.push_back(cell);
        }

        return table;

    }

    Database::IndexInteriorPage read_index_interior_page (size_t page_offset) {
        std::cout << "===== START read index interior " << std::endl;
        Database::IndexInteriorPage page;
        //read cell count
        uint32_t cell_count = Database::read_2_bytes_from_db(Database::db, page_offset + 3);
        page.cell_count = cell_count;

        //read rightmost pointer
        uint32_t rightmost_pointer = Database::read_4_bytes_from_db(Database::db, page_offset + 8);
        page.rightmost_pointer = rightmost_pointer;

        std::cout << "(2) cell count: " << cell_count << " rightmost pointer: " << rightmost_pointer << std::endl;
        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::INTERIOR;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            //std::cout << "cell pointers: " <<  page_offset << "+" << Decode::to_uint16_t(num) << std::endl;
            cell_offset += 2;
        }
        //read index interior cell
        // 1. 4 bytes left_child_pointer
        // 2. payload size (in bytes)
        // 3. payload
        // 4. 4 bytes first_overflow_page
        std::vector<Database::IndexInteriorCell> cells;
        for (auto &offset: cell_offsets) {
            uint32_t left_child_pointer = Database::read_4_bytes_from_db(Database::db, offset);

            size_t payload_size_offset = offset + 4;
            size_t payload_offset = offset + 4;
            uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);

            std::vector<Database::RowField> field = Database::read_row(payload_offset);

            //std::cout << "payload size: " << payload_size << " field[0] " << field[0].field_size << " field[1] " <<   field[1].field_size << std::endl;
            assert(payload_size - 3 == field[0].field_size + field[1].field_size);
            Database::IndexInteriorCell cell = {
                .left_child_pointer = left_child_pointer, 
                .payload_size = payload_size,
                .field = field
            };
            cells.push_back(cell);
        }
        page.cells = cells;

        std::cout << "===== END read index interior " << std::endl;
        return page;
        
    }

    Database::IndexLeafPage read_index_leaf_page (size_t page_offset) {
        Database::IndexLeafPage page;
        //read cell count
        uint16_t cell_count = Database::read_2_bytes_from_db(Database::db, page_offset+3);
        page.cell_count = cell_count;

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::LEAF;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            //std::cout << "cell pointers: " <<  page_offset + Decode::to_uint16_t(num) << std::endl;
            cell_offset += 2;
        }

        //read index leaf cell
        // 1. payload size (in bytes)
        // 2. payload
        // 3. 4 bytes first_overflow_page
        std::vector<Database::IndexLeafCell> cells;
        for (auto &offset: cell_offsets) {
            size_t payload_offset = offset;
            uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
            //std::cout << "offsets: " << rowid_offset << " " << payload_offset << std::endl;
            std::vector<Database::RowField> field = Database::read_row(payload_offset);

            Database::IndexLeafCell cell = {
                .payload_size = payload_size,
                .field = field
            };
            cells.push_back(cell);
        }
        page.cells = cells;
        return page;
    }

    size_t read_row_id (Database::RowField &field) {
        size_t row_id;
        if (field.field_type == 2) {
            row_id = Decode::to_uint16_t(static_cast<char*>(field.field_value));
        }
        else if (field.field_type == 3) {
            row_id = Decode::deserialize_24_bit_to_signed(static_cast<char*>(field.field_value));
        }
        else {
            std::cout << "row_id type: " << field.field_type << std::endl;
            throw std::logic_error("unknown field type.");
        }
        return row_id;
    }
    void process_table_interior (
        Database::TableInteriorPage &page, 
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
        ) {
        //std::cout << "5 cell count " << page.cell_count << std::endl;
        for (auto &c: page.cells) {
            
            const size_t page_offset = (c.left_child_pointer - 1) * page_size;
            uint16_t page_type = Database::read_1_byte_from_db(Database::db, page_offset);

            //std::cout << "page type: " << page_type << " lcp: " << c.left_child_pointer << " page offset: " << page_offset << " rowid: " << c.rowid << std::endl;
            
            if (page_type == Database::PageType::TABLE_LEAF) {
                std::vector<Database::TableLeafCell> leaf_table = read_table_leaf_cell(page_offset);
                fetch_query_result(query, def, leaf_table, c.rowid);

            } else if (page_type == Database::PageType::TABLE_INTERIOR) {
                Database::TableInteriorPage interior_table = read_table_interior_page(page_offset);
                process_table_interior(interior_table, page_size, query, def);
            }
        }

        //process rightmost pointer
        const size_t page_offset = (page.rightmost_pointer - 1) * page_size;
        
        uint16_t page_type = Database::read_1_byte_from_db(Database::db, page_offset);
        //std::cout << " page_type: " << page_type << std::endl;
        if (page_type == Database::PageType::TABLE_LEAF) {
            std::vector<Database::TableLeafCell> leaf_table = read_table_leaf_cell(page_offset);
            fetch_query_result(query, def, leaf_table, 0);
        } else if (page_type == Database::PageType::TABLE_INTERIOR) {
            Database::TableInteriorPage interior_table = read_table_interior_page(page_offset);
            process_table_interior(interior_table, page_size, query, def);
        }
    }

    void print_record_from_leaf_cell (
        Database::TableLeafCell &row, 
        std::vector<std::string> &columns,
        std::vector<WhereCondition> &conditions,
        std::map<std::string, int> map_column_position
    ) {
        bool skip_row = false;     
        for(size_t i = 0; i < conditions.size(); ++i) {
            
            Database::RowField row_field;
            std::string expression;
            std::string row_value;
            if (map_column_position[conditions[i].column] == -1) { //if condition == id
                row_field.field_size = -1;
                row_field.field_value = &row.rowid;
            
            } else if (map_column_position[conditions[i].column] < row.field.size()) {
                row_field = row.field[map_column_position[conditions[i].column]];
                if (row_field.field_type == Database::SerialType::SERIAL_NULL) {
                    skip_row = true;
                    continue;
                } 
            } 
            expression = conditions[i].expression;
            row_value = conditions[i].value;   
            if (expression == "=") {
                
                if (row_field.field_size == -1 && stoi(row_value) != *static_cast<uint32_t*>(row_field.field_value)) {
                    skip_row = true;
                }
                if (row_field.field_size != -1 && row_value != *static_cast<std::string*>(row_field.field_value)) {
                    skip_row = true;
                }
            }
            
        }
        if (skip_row) {
            return;
        }

        //std::cout << "offset: " << row.offset <<  " ";
        for (size_t i = 0; i < columns.size(); ++i) {
            Database::RowField field;
            if (map_column_position[columns[i]] == -1) { //if condition == id
                field.field_size = 0;
                field.field_value = &row.rowid;
                std::cout << std::to_string(row.rowid);
            } else if (map_column_position[columns[i]] < row.field.size()) {
                field = row.field[map_column_position[columns[i]]];
                if (field.field_type == Database::SerialType::SERIAL_NULL) {
                    std::cout << "NULL";
                } else {
                    std::cout << *static_cast<std::string*>(field.field_value);
                }
            } 
            
            if(columns.size() > 1 && i != columns.size() - 1) {
                std::cout << '|';
            }
        }
        std::cout << std::endl;
    }

    void fetch_query_result (
        const DQLStatement &query, 
        const DDLStatement &table_def, 
        std::vector<Database::TableLeafCell> &table,
        uint32_t id
    ) {
        std::map<std::string, int> map_column_position;

        for(size_t i = 0; i < table_def.columns.size(); ++i) {
            if (table_def.additional_statement[i] == "primary key autoincrement") {
                map_column_position[table_def.columns[i]] = -1; //indicate the column is a primary key
            } else {
                map_column_position[table_def.columns[i]] = i; 
            }
        }
        
        
        //output table entries according to query 
        std::vector<std::string> columns = query.columns;
        std::vector<WhereCondition> conditions = query.condition;

        if (query.columns.size() == 1 && query.columns[0] == "*") { //select all columns
            columns = table_def.columns;
        }

        int counter = 0;
        for (auto &row: table) {
            /*
            std::cout << "size: " << row.field.size() << std::endl;
            for (auto f: row.field) {
                std::cout << f.field_type << " - " << f.field_size << " - ";
                if (f.field_type == Database::SerialType::SERIAL_NULL) {
                    std::cout << "NULL" << '|';
                }
                else {
                    std::cout << *static_cast<std::string*>(f.field_value) << '|';
                }
            }
            std::cout << std::endl;
            
            bool skip_row = false;     
            for(size_t i = 0; i < conditions.size(); ++i) {
                
                Database::RowField row_field;
                std::string expression;
                std::string row_value;
                if (map_column_position[conditions[i].column] == -1) { //if condition == id
                    row_field.field_size = -1;
                    row_field.field_value = &row.rowid;
                
                } else if (map_column_position[conditions[i].column] < row.field.size()) {
                    row_field = row.field[map_column_position[conditions[i].column]];
                    if (row_field.field_type == Database::SerialType::SERIAL_NULL) {
                        skip_row = true;
                        continue;
                    } 
                } 

                expression = conditions[i].expression;
                row_value = conditions[i].value;   
                if (expression == "=") {
                    
                    if (row_field.field_size == -1 && stoi(row_value) != *static_cast<uint32_t*>(row_field.field_value)) {
                        skip_row = true;
                    }

                    if (row_field.field_size != -1 && row_value != *static_cast<std::string*>(row_field.field_value)) {
                        skip_row = true;
                    }

                }
                
            }
            if (skip_row) {
                continue;
            }
            std::cout << "offset: " << row.offset <<  " ";

            for (size_t i = 0; i < columns.size(); ++i) {
                Database::RowField field;
                if (map_column_position[columns[i]] == -1) { //if condition == id
                    field.field_size = 0;
                    field.field_value = &row.rowid;
                    std::cout << std::to_string(row.rowid);
                } else if (map_column_position[columns[i]] < row.field.size()) {
                    field = row.field[map_column_position[columns[i]]];
                    if (field.field_type == Database::SerialType::SERIAL_NULL) {
                        std::cout << "NULL";
                    } else {
                        std::cout << *static_cast<std::string*>(field.field_value);
                    }
                } 
                
                if(columns.size() > 1 && i != columns.size() - 1) {
                    std::cout << '|';
                }
            }

            std::cout << std::endl; 
            */
            print_record_from_leaf_cell(row, columns, conditions, map_column_position);
        }

    }

    void fetch_query_result_index_scan (
        const DQLStatement &query, 
        const DDLStatement &table_def, 
        std::vector<Database::TableLeafCell> &records
    ) {
        std::map<std::string, int> map_column_position;

        for(size_t i = 0; i < table_def.columns.size(); ++i) {
            if (table_def.additional_statement[i] == "primary key autoincrement") {
                map_column_position[table_def.columns[i]] = -1; //indicate the column is a primary key
            } else {
                map_column_position[table_def.columns[i]] = i; 
            }
        }
        
        // for (auto &a: map_column_position) {
        //     std::cout << a.first << ' ' << a.second << std::endl;
        // }
        
        //output table entries according to query 
        std::vector<std::string> columns = query.columns;
        std::vector<WhereCondition> conditions = query.condition;

        if (query.columns.size() == 1 && query.columns[0] == "*") { //select all columns
            columns = table_def.columns;
        }

    }

    void process_index_interior (
        Database::IndexInteriorPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    ) {
        //std::cout << "process_index_interior() cell size: " << page.cells.size() << std::endl;
        for(auto cell: page.cells) {
            const size_t page_offset = (cell.left_child_pointer - 1) * page_size;
            //std::cout << "(2) lcp: " << cell.left_child_pointer << " offset: "<< page_offset;
            //std::cout << std::endl;
            
            uint16_t page_type = Database::read_1_byte_from_db(Database::db, page_offset);

            char* id = static_cast<char*>(cell.field[1].field_value);
            std::cout 
            << " page_type: " << page_type 
            << " country: " << *static_cast<std::string*>(cell.field[0].field_value) 
            << " row id: " << Decode::deserialize_24_bit_to_signed(id)
            // << " page id serial type: " << cell.field[1].field_type
            << std::endl;

            if (page_type == Database::PageType::INDEX_INTERIOR) {
                //std::cout << "INDEX INTERIOR page_offset " << page_offset << std::endl;
                Database::IndexInteriorPage index_interior = read_index_interior_page(page_offset);
                process_index_interior(index_interior, page_size, query, def);
            }
            else if (page_type == Database::PageType::INDEX_LEAF) {
                //std::cout << "INDEX LEAF page_offset " << page_offset << std::endl;
                Database::IndexLeafPage index_leaf;
                index_leaf = read_index_leaf_page(page_offset);
                process_index_leaf(index_leaf, page_size, query, def);

            }
        }

        //process rightmost pointer
        const size_t page_offset = (page.rightmost_pointer - 1) * page_size;
        uint16_t page_type = Database::read_1_byte_from_db(Database::db, page_offset);

        std::cout << "rmp: " << page.rightmost_pointer << " type: "<< page_type << std::endl;

        if (page_type == Database::PageType::INDEX_INTERIOR) {
                Database::IndexInteriorPage index_interior = read_index_interior_page(page_offset);
                process_index_interior(index_interior, page_size, query, def);
        }
        else if (page_type == Database::PageType::INDEX_LEAF) {
            Database::IndexLeafPage index_leaf = read_index_leaf_page(page_offset);
            process_index_leaf(index_leaf, page_size, query, def);
        }

    }

    void process_index_leaf (
        Database::IndexLeafPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    ) {
        //std::cout << "index leaf cell size: " << page.cell_count << std::endl;
        for(auto cell: page.cells) {
            char* id = static_cast<char*>(cell.field[1].field_value);
            int32_t decoded_id;
            
            if (*static_cast<std:: string*>(cell.field[0].field_value) == "chad") {
                if (cell.field[1].field_size == 2) {
                    decoded_id = Decode::to_uint16_t(id);
                }
                else if (cell.field[1].field_size == 3) {
                    decoded_id = Decode::deserialize_24_bit_to_signed(id);
                }
                std::cout 
                << " field[1] size: " << cell.field[1].field_size
                << " country: " << *static_cast<std:: string*>(cell.field[0].field_value) 
                << " row id: " << decoded_id
                //<< " page id 8 bit: " << static_cast<uint16_t>(id[0])
                //<< " serial type: " << cell.field[1].field_size
                << std::endl;

            }      
            else {
                //std::cout << *static_cast<std:: string*>(cell.field[0].field_value) << std::endl;
            }
        }
    }

    std::vector<size_t> find_row_id_from_index(
        size_t page_offset,
        const uint32_t page_size, 
        const Query::DQLStatement &query,
        const Query::DDLStatement &def,
        int depth) {
        
        //std::cout << "HEYYY" << std::endl;
        std::string target_table = def.index_target;
        std::string index_key = def.index_key;

        std::vector<WhereCondition> conditions = query.condition;
        if (query.table != target_table) {
            std::cout << "query table and index table mismatch " << query.table << " " << target_table << std::endl;
            return {};
        }
        bool can_use_index = false;
        std::string query_key;
        for (auto &c: conditions) {
            if(c.column == index_key) {
                can_use_index = true;
                query_key = c.value;
                break;
            }
        }
        if (!can_use_index) {
            std::cout << "query condition has no index" << std::endl;
            return {};
        }

        uint16_t page_type = Database::read_1_byte_from_db(Database::db, page_offset);
        uint32_t cell_count = Database::read_2_bytes_from_db(Database::db, page_offset + 3);

        //read rightmost pointer
        uint32_t rightmost_pointer;
        if (page_type == Database::INDEX_INTERIOR) {
            rightmost_pointer = Database::read_4_bytes_from_db(Database::db, page_offset + 8);
            
        } 

        //jump the headers
        size_t cell_offset;
        if (page_type == Database::INDEX_INTERIOR) {
            cell_offset = page_offset + Database::HeaderSize::INTERIOR;
            //std::cout << page_type << " INDEX INTERIOR cell count: " << cell_count << " start: " << cell_offset << " depth: " << depth << std::endl;
        } else if (page_type == Database::INDEX_LEAF) {
            cell_offset = page_offset + Database::HeaderSize::LEAF;
            //std::cout << page_type << " INDEX LEAF cell count: " << cell_count << " start: " << cell_offset << " depth: " << depth << std::endl;
        }

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            cell_offset += 2;
        }

        //binary search
        int32_t start = 0;
        int32_t end = cell_offsets.size()-1;
        int32_t mid = start+(end-start)/2;

        //the query key will not lie on the cell that has smaller key.

        std::vector<size_t> row_ids;
        if (page_type == Database::INDEX_INTERIOR) {

            //we can skip cell binary search altogether and jump to rightmost pointer if the last cell key < query key
            //read cell at mid
            size_t last_offset = cell_offsets.back() + 4;
            uint32_t payload_size = Decode::read_varint_new(Database::db, last_offset);
            std::vector<Database::RowField> field = Database::read_row(last_offset);
            std::string cell_key = *static_cast<std::string*>(field[0].field_value);

            //std::cout << cell_key << std::endl;
            
            if (cell_key >= query_key) {
                while (start <= end) {
                    //read cell at mid
                    size_t offset = cell_offsets.at(mid);
                    size_t payload_offset = offset + 4;
                    uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
                    std::vector<Database::RowField> field = Database::read_row(payload_offset);
                    std::string cell_key = *static_cast<std::string*>(field[0].field_value);
                    size_t row_id = read_row_id (field[1]);

                    bool cond = cell_key < query_key;
                    //std::cout << "cell key: " << cell_key << " query_key: " << query_key  << " row_id: " << row_id << " cell key < query key " << cond;
                    //std::cout << " bin-search indexes:" << start << " " << mid << " " << end << std::endl;
                    if (cell_key < query_key) {
                        start = mid+1;
                        mid = start+(end-start)/2;
                    }
                    else if (cell_key > query_key) {
                        end = mid-1;
                        mid = start+(end-start)/2;
                    }
                    else {
                        break;
                    }
                } 

                //regardless key found or not, go linearly to the lower bound (down) and upper bound (up) of our mid
                
                size_t offset = cell_offsets.at(mid);

                uint32_t left_child_pointer = Database::read_4_bytes_from_db(Database::db, offset);
                size_t payload_offset = offset + 4;
                uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
                std::vector<Database::RowField> field = Database::read_row(payload_offset);
                std::string cell_key = *static_cast<std::string*>(field[0].field_value);
                size_t row_id = read_row_id (field[1]);

                //std::cout << "start searching lower and upper bound on " << cell_key << " " << row_id << std::endl;

                if (cell_key == query_key) {
                     row_ids.push_back(row_id);
                }

                size_t lcp_offset = (left_child_pointer - 1) * page_size;
                //std::cout << "offset: " << offset << " lcp offset: " << lcp_offset << std::endl;
                std::vector<size_t> r = find_row_id_from_index(lcp_offset, page_size, query, def, ++depth);
                row_ids.insert(row_ids.end(), r.begin(), r.end());

                //go lower bound
                int32_t mid_down = mid-1;
                while (mid_down >= 0) {
                    size_t offset = cell_offsets.at(mid_down--);
                    uint32_t lcp_down = Database::read_4_bytes_from_db(Database::db, offset);
                    size_t payload_offset = offset + 4;
                    uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
                    std::vector<Database::RowField> field = Database::read_row(payload_offset);
                    std::string cell_key_down = *static_cast<std::string*>(field[0].field_value); 
                    size_t row_id_down = read_row_id (field[1]);

                    //std::cout << "last try down: " << cell_key_down << " " << query_key << " at: " << row_id_down << std::endl;
                    if(cell_key_down < query_key) {
                        break;
                    } 
                    if (cell_key == query_key) {
                        row_ids.push_back(row_id);
                    }
                    size_t lcp_offset = (lcp_down - 1) * page_size;
                    std::vector<size_t> r_down = find_row_id_from_index(lcp_offset, page_size, query, def, ++depth);
                    row_ids.insert(row_ids.end(), r_down.begin(), r_down.end());
                }  

                //go upper bound
                int32_t mid_up = mid+1;
                while (mid_up < cell_offsets.size()) {
                    size_t offset = cell_offsets.at(mid_up++);
                    uint32_t lcp_up = Database::read_4_bytes_from_db(Database::db, offset);
                    size_t payload_offset = offset + 4;
                    uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
                    std::vector<Database::RowField> field = Database::read_row(payload_offset);
                    std::string cell_key_up = *static_cast<std::string*>(field[0].field_value); 
                    size_t row_id_up = read_row_id (field[1]);

                    //std::cout << "last try up: " << cell_key_up << " " << query_key << std::endl;
                    size_t lcp_offset = (lcp_up - 1) * page_size;
                    std::vector<size_t> r_up = find_row_id_from_index(lcp_offset, page_size, query, def, ++depth);  
                    if (r_up.size() == 0) {
                        break;
                    } else if (cell_key_up == query_key) {
                        row_ids.push_back(row_id_up);
                    }

                    row_ids.insert(row_ids.end(), r_up.begin(), r_up.end());
                
                }
            }
            //rightmost pointer
            //std::cout << "rmp find_row_id() " << rightmost_pointer << std::endl;
            size_t rmp_offset = (rightmost_pointer - 1) * page_size;
            std::vector<size_t> r_rmp = find_row_id_from_index(rmp_offset, page_size, query, def, ++depth);
            row_ids.insert(row_ids.end(), r_rmp.begin(), r_rmp.end());
        }
        else if (page_type == Database::INDEX_LEAF) {
            
            //std::cout << "INDEX LEAF cell size: " << cell_count << std::endl;
            while (start <= end) {
                //read cell at mid
                size_t offset = cell_offsets.at(mid);
    
                size_t payload_offset = offset;
                uint32_t payload_size = Decode::read_varint_new(Database::db, payload_offset);
                std::vector<Database::RowField> field = Database::read_row(payload_offset);
                std::string cell_key = *static_cast<std::string*>(field[0].field_value); 

                //std::cout << "INDEX LEAF BINARY SEARCH " << start << " " << mid << " " << end << " key: " << cell_key  << std::endl;

                if (cell_key < query_key) {
                    start = mid+1;
                    mid = start+(end-start)/2;                   
                } else if (cell_key > query_key) {
                    end = mid-1;
                    mid = start+(end-start)/2; 
                } else  {
                    assert(field[1].field_type == 2 || field[1].field_type == 3);
                    size_t row_id = read_row_id (field[1]);                    
                    row_ids.push_back(row_id);
                    //std::cout << "row id for: " << cell_key << " " << row_id << std::endl;

                    int32_t mid_down = mid-1;
                    std::string cell_key_down = cell_key;
                    size_t row_id_down = 0;

                    while(mid_down >= 0) {
                        size_t offset = cell_offsets.at(mid_down--);
                        uint32_t payload_size = Decode::read_varint_new(Database::db, offset);
                        std::vector<Database::RowField> field = Database::read_row(offset);
                        cell_key_down = *static_cast<std::string*>(field[0].field_value); 
                        
                        if(cell_key_down != query_key) {
                            break;
                        }
                        row_id_down = read_row_id (field[1]);
                        row_ids.push_back(row_id_down);
                        //std::cout << "row id down for: " << cell_key_down << " " << row_id_down << std::endl;
                    }

                    int32_t mid_up = mid+1;
                    std::string cell_key_up = cell_key;
                    size_t row_id_up = 0;
                    
                    while (cell_key_up == query_key && mid_up < cell_offsets.size()) {
                        size_t offset = cell_offsets.at(mid_up++);

                        uint32_t payload_size = Decode::read_varint_new(Database::db, offset);
                        std::vector<Database::RowField> field = Database::read_row(offset);
                        cell_key_up = *static_cast<std::string*>(field[0].field_value);
                        
                        
                        if(cell_key_up != query_key) {
                            break;
                        }

                        row_id_up = read_row_id (field[1]);
                        row_ids.push_back(row_id_up);
                        //std::cout << "row id up for: " << cell_key_up << " " << row_id_up << std::endl;
                        
                    }
                    break;
                } 
            }
        }
        return row_ids;
    }
    
    std::vector<Database::TableLeafCell> do_index_scan (
        std::vector<size_t> &row_ids,
        const Query::DQLStatement &query,
        const DDLStatement &table_def,
        const uint32_t page_size, 
        Database::PageHeader &table_to_process
    ) {
        std::string table_name = table_to_process.name;
        size_t page_offset = table_to_process.page_offset;
        uint16_t page_type = table_to_process.page_type;
        uint16_t cell_count = table_to_process.cell_count;

        //std::cout << "type: " << page_type << " offset: " <<page_offset << " count: " << cell_count << std::endl;
        assert(page_type == 5 || page_type == 13);

        uint32_t rightmost_pointer;
        size_t cell_offset;
        if (page_type == Database::PageType::TABLE_INTERIOR) {
            rightmost_pointer = Database::read_4_bytes_from_db(Database::db, page_offset + 8);
            cell_offset = page_offset + Database::HeaderSize::INTERIOR;
        } 
        else if (page_type == Database::PageType::TABLE_LEAF) {
            cell_offset = page_offset + Database::HeaderSize::LEAF;
        }

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            cell_offsets.push_back(page_offset + Database::read_2_bytes_from_db(Database::db, cell_offset)); 
            cell_offset += 2;
        }
        // std::cout << "cell count: " << cell_count << std::endl;
        // return 0;

        // for (auto cell_offset: cell_offsets) {
        //     size_t o = cell_offset + 4;
        //     auto row_id = Decode::read_varint_new(Database::db, o);
        //     std::cout << cell_offset << " has rowid: " << row_id << std::endl;
        // }
        // return 0;

        //binary search
        /*
        for every row id in row id array

        check for row id just like on the index table
        if page type = 5 then
            if cell_row_id < row_id then search on the right
            else 
                check on current cell
                check on cell - 1 while cell_row_id >= row_id
                check on cell + 1 while row result != 0
        else if page type = 13 then
            if cell_row_id < row_id then search on the right
            if cell row id > row_id then search on the left
            else
                print current cell
        */

        int record_printed = 0;
        std::vector<Database::TableLeafCell> records;

        for(auto &row_id: row_ids) {
            int32_t start = 0;
            int32_t end = cell_offsets.size()-1;
            int32_t mid = start+(end-start)/2;
            //std::cout << "looking for row id: " << row_id << std::endl;
            if (page_type == Database::PageType::TABLE_INTERIOR) {
                size_t last_offset = cell_offsets.back() + 4;
                uint32_t last_row_id = Decode::read_varint_new(Database::db, last_offset);

                //std::cout << "last row id: " << last_row_id << std::endl;

                if (last_row_id >= row_id) {
                    while (start <= end) {
                        size_t offset = cell_offsets.at(mid);
                        size_t row_id_offset =  offset + 4;
                        uint32_t cell_row_id = Decode::read_varint_new(Database::db, row_id_offset);

                        //std::cout << "s: " << start << " e: " << end << " cell (mid) row id: " << cell_row_id << " row id: " << row_id << std::endl;
                        if (cell_row_id < row_id) {
                            start = mid + 1;
                            mid = start + (end-start)/2;
                        } 
                        else {
                            size_t lcp_offset = (Database::read_4_bytes_from_db(Database::db, offset) - 1) * page_size;
                            std::vector<size_t> single_row_id = {row_id};
                            Database::PageHeader header;
                            
                            header.cell_count = Database::read_2_bytes_from_db(Database::db, lcp_offset + 3);
                            header.page_offset = lcp_offset;
                            header.page_type = Database::read_1_byte_from_db(Database::db, lcp_offset);
                            
                            assert(header.page_type == 5 || header.page_type == 13);

                            //std::cout << "search " << row_id << " at: " << header.page_offset << " with type: " << header.page_type << std::endl;
    
                            //int result = fetch_query_result_index_scan(single_row_id, query, table_def, page_size, header);
                            std::vector<Database::TableLeafCell> r = do_index_scan(single_row_id, query, table_def, page_size, header);

                            if (r.size() != 0) {
                                //record_printed += result;
                                records.insert(records.end(), r.begin(), r.end());
                                break;
                            }
                            
                            int32_t mid_down = mid-1;
                            uint32_t lcp_offset_down;
                            uint32_t row_id_down;
                            std::vector<Database::TableLeafCell> result_down;
    
                            while (mid_down >= 0) {
                                size_t offset = cell_offsets.at(mid_down--);
                                lcp_offset_down = (Database::read_4_bytes_from_db(Database::db, offset) - 1) * page_size;
                                size_t row_id_offset = offset + 4;
                                row_id_down = Decode::read_varint_new(Database::db, row_id_offset);
    
                                if (row_id_down < row_id) {
                                    break;
                                }
                                Database::PageHeader header;
                                header.cell_count = Database::read_2_bytes_from_db(Database::db, lcp_offset_down + 3);
                                header.page_offset = lcp_offset_down;
                                header.page_type = Database::read_1_byte_from_db(Database::db, lcp_offset_down);
                                
                                assert(header.page_type == 5 || header.page_type == 13);
    
                                result_down = do_index_scan(single_row_id, query, table_def, page_size, header);
                                if (result_down.size() != 0) {
                                    records.insert(records.end(), result_down.begin(), result_down.end());
                                    //record_printed += result_down;
                                    break;
                                }
                            }
    
                            int32_t mid_up = mid+1;
                            uint32_t lcp_offset_up = lcp_offset;
                            uint32_t row_id_up;
                            std::vector<Database::TableLeafCell> result_up;
    
                            while (mid_up < cell_offsets.size()) {
                                size_t offset = cell_offsets.at(mid_up++);
                                lcp_offset_up = (Database::read_4_bytes_from_db(Database::db, offset) - 1) * page_size;

                                Database::PageHeader header;
                                
                                header.cell_count = Database::read_2_bytes_from_db(Database::db, lcp_offset_up + 3);
                                header.page_offset = lcp_offset_up;
                                header.page_type = Database::read_1_byte_from_db(Database::db, lcp_offset_up);
                                
                                assert(header.page_type == 5 || header.page_type == 13);
    
                                result_up = do_index_scan(single_row_id, query, table_def, page_size, header);
                                // if (result_up != 0) {
                                //     break;
                                // }
                                records.insert(records.end(), result_up.begin(), result_up.end());
                                //record_printed += result_up;
                                break;
                            }
                            break;
                        }
                    }
                }
                //process rightmost pointer
                
                size_t rmp_offset = (rightmost_pointer - 1) * page_size;
                Database::PageHeader header;
                header.page_offset = rmp_offset;
                header.cell_count = Database::read_2_bytes_from_db(Database::db, rmp_offset + 3);
                header.page_type = Database::read_1_byte_from_db(Database::db, rmp_offset);
                
                std::vector<size_t> single_row_id = {row_id};
                //std::cout << "rightmost pointer: " << rmp_offset << std::endl;
                std::vector<Database::TableLeafCell> rmp_result = do_index_scan(single_row_id, query, table_def, page_size, header);
                records.insert(records.end(), rmp_result.begin(), rmp_result.end());
                //record_printed += rmp_result;
            } 
            else if (page_type == Database::PageType::TABLE_LEAF) {
                while (start <= end) {
                    size_t offset = cell_offsets.at(mid);
                    size_t var_offset = offset;
                    uint32_t payload_size = Decode::read_varint_new(Database::db, var_offset);
                    uint32_t cell_row_id = Decode::read_varint_new(Database::db, var_offset);
                    //std::cout << "table leaf cell row id: " << cell_row_id << " row id: " << cell_row_id << std::endl;

                    if (cell_row_id < row_id) {
                        start = mid+1;
                        mid = start+(end-start)/2;
                    }
                    else if (cell_row_id > row_id) {
                        end = mid-1;
                        mid = start+(end-start)/2;
                    }
                    else if (cell_row_id == row_id){
                        //std::cout << "FOUND: " << cell_row_id << " at: " << offset << std::endl;

                        //print current cell 
                        Database::TableLeafCell cell;
                        cell.rowid = row_id;
                        cell.offset = offset;
                        cell.payload_size = payload_size;
                        cell.field = Database::read_row(var_offset);
                        
                        records.push_back(cell);
                        break;
                    }
                }
            }
        }       
        return records;
    }

    void process_select_from_statement(
         const DQLStatement &query, 
         const std::map<std::string, std::vector<Database::PageHeader>> &table_map,
         uint32_t page_size,
         bool use_index

    ) {
        
        auto tables = table_map.at(query.table);
        Database::PageHeader index_table;
        Database::PageHeader real_table;
        DDLStatement index_table_def, real_table_def;
        for(auto table: tables) { 
            //std::cout << table.name << " " << table.type << " " << table.page_type << std::endl;
            if (table.type == "index" && use_index) {
                index_table = table;
                index_table_def = parse_table_definition(index_table.name, index_table.definition);
            } else if (table.type == "table") {
                real_table = table;
                real_table_def = parse_table_definition(real_table.name, real_table.definition);
            }
        }


        Database::PageHeader table_to_process;
        bool can_use_index = false;
        std::string query_key;
        DDLStatement def;
        if (index_table.name.size() != 0) {
            std::vector<WhereCondition> conditions = query.condition;
            // if (query.table != index_table_def.index_target) {
            //     std::cout << query.table << " " << index_table_def.index_target << std::endl;
            //     std::cout << "query table and index table mismatch " <<  std::endl;
            //     return;
            // }

            for (auto &c: conditions) {
                if(c.column == index_table_def.index_key) {
                    can_use_index = true;
                    query_key = c.value;
                    break;
                }
            }
            if (!can_use_index) {
                table_to_process = real_table;
                def = real_table_def;
                //std::cout << "query condition has no index" << std::endl;
            } else {
                table_to_process = index_table;
                def = index_table_def;
            }
        } else {
            //std::cout << "NO INDEX " << real_table.page_type << std::endl;
            table_to_process = real_table;
            def = real_table_def;
        }


        const size_t page_offset = table_to_process.page_offset;
        uint16_t page_type = table_to_process.page_type;
        uint16_t cell_count = table_to_process.cell_count;

        if (page_type == Database::PageType::TABLE_LEAF 
        && query.columns.size() == 1 
        && query.columns[0] == "count(*)"
        ) {
            printf("%u\n", cell_count);
            return;
        }
        else if (query.columns.size() > 0) {
            if (page_type == Database::PageType::TABLE_LEAF) { //full table scan
                //if(tables.size() > 1 && tables[1].name == "companies") std::cout << "huh" << std::endl;
                std::vector<Database::TableLeafCell> table = read_table_leaf_cell(page_offset);
                fetch_query_result(query, def, table, 0);

            } else if (page_type == Database::PageType::TABLE_INTERIOR) {  //full table scan
                Database::TableInteriorPage page = read_table_interior_page(page_offset);
                process_table_interior(page, page_size, query, def);

            } else if (page_type == Database::PageType::INDEX_INTERIOR) { //index table scan
                //old full table scan on index table
                // Database::IndexInteriorPage page = read_index_interior_page(page_offset);
                // process_index_interior(page, page_size, query, def);

                //binary search for row ids in index table
                std::vector<size_t> rows = find_row_id_from_index(page_offset, page_size, query, def, 0);
                
                //std::cout << "row id size: " << rows.size() << std::endl;
                //for (auto r: rows) {std::cout << "row id: " << r << std::endl;}

                //binary search for real records from row id in ordinary table
                auto tables = table_map.at(query.table);
                Database::PageHeader table_to_process;
                for(auto &table: tables) {
                    if (table.type == "table") {
                        table_to_process = table;
                        break;
                    }
                    table_to_process = table;
                }
                auto record_printed = do_index_scan (rows, query, def, page_size, table_to_process);
                //std::cout << "record printed: " << record_printed.size() << std::endl;
                // for (auto r: record_printed) {std::cout << "row id: " << r.rowid << std::endl;}

                //print all targeted cells
                fetch_query_result(query, real_table_def, record_printed, 0);
                
            } 
        }
        
    }

/* FUNCTIONS TO PARSE QUERY AND TABLE DEFINITION */

    std::string lower_string (std::string s) {
        std::string result = s;
        std::transform(result.begin(), result.end(), result.begin(),
        [](unsigned char c){ return std::tolower(c); });
        return result;
    }

    std::string get_where_condition_string_value (std::string &s) {
        std::stringstream ss(s);
        ss >> std::ws;
        std::string _;
        std::getline(ss, _, '\'');
        std::string value;
        std::getline(ss, value, '\'');
        return value;
    }

    DQLStatement parse_query (const std::string &q) {
        DQLStatement parsed_query;
        std::stringstream ss(q);
        ss >> std::ws;
        std::string command;
        std::getline(ss, command, ' ');
        if (lower_string(command) == "select") {
            size_t get_pos = ss.tellg();
            parsed_query.columns = get_column(q, get_pos);
            parsed_query.table = get_table_name(q, get_pos);
            parsed_query.condition = get_where_clause(q, get_pos);
            return parsed_query;
        }
        else {
            throw std::logic_error("Not yet implemented.");
        }

    }

    std::vector<std::string> get_column (const std::string &query, size_t &offset) {
        std::vector<std::string> columns;
        std::stringstream ss(query);
        ss.seekg(offset);
        ss >> std::ws;
        size_t start = offset;
        std::string cols, line;
        while (std::getline(ss, line, ' ') && lower_string(line) != "from") {}
        if(lower_string(line) != "from") {
            throw std::logic_error("get_column() expects FROM clause.");
        }
        size_t end = ss.tellg();
        end -= 5;
        cols = ss.str().substr(start, end-start-1);
        offset = end;
        std::stringstream ss_cols(cols);
        std::string token;
        while (std::getline(ss_cols, token, ',')) {
            size_t start = token.find_first_not_of(' ');
            size_t end = token.find_last_not_of(' ');
            token = (start == std::string::npos) ? "": token.substr(start, end-start+1);
            columns.push_back(token);
        }

        return columns;
    }

    std::string get_table_name (const std::string &query, size_t &offset) {
        std::stringstream ss(query);
        ss.seekg(offset);
        std::string command_FROM;
        ss >> std::ws;
        std::getline(ss, command_FROM, ' ');
        if (lower_string(command_FROM) == "from") {
            std::string table;
            ss >> std::ws;
            std::getline(ss, table, ' ');
            offset = ss.tellg();
            return table;
        }
        else {
            throw std::logic_error("Expected FROM clause.");
        }
    }

    std::vector<WhereCondition> get_where_clause (
        const std::string &query, 
        size_t &offset
        ) {
        std::vector<WhereCondition> conditions;
        std::stringstream ss(query);
        ss.seekg(offset);
        std::string command_WHERE;
        ss >> std::ws;
        std::getline(ss, command_WHERE, ' ');
        if (lower_string(command_WHERE) == "where") {

            WhereCondition cond;
            ss >> std::ws;
            std::getline(ss, cond.column, ' ');
            std::getline(ss, cond.expression, ' ');
            std::string start;
            std::getline(ss, start, '\'');
            if (start.size() == 0) {
                std::getline(ss, cond.value, '\'');
            }
            else {
                std::getline(ss, cond.value, ' ');
            }
            conditions.push_back(cond);
            offset = ss.tellg();
            return conditions;
        }
        else {
            return {};
        }
    }

    DDLStatement parse_table_definition (
        const std::string &table_name,
        const std::string &table_definition
    ) {

        DDLStatement def;
        std::stringstream ss(table_definition);
        ss >> std::ws;
        std::string command;
        std::getline(ss, command, ' ');
        if (lower_string(command) != "create") {
            std::logic_error("Not a table definition");
        }

        std::string type;
        std::getline(ss, type, ' ');
        def.definition_of = type;

        // ss >> std::ws;
        // std::string name;
        // std::getline(ss, name, '\n');
        // def.table = name;

        if (lower_string(def.definition_of) == "index") {
            ss >> std::ws;
            std::string on;
            std::getline(ss, on, ' ');

            if (lower_string(on) != "on") {
                std::logic_error("Invalid index table definition");
            }

            std::string target;
            ss >> std::ws;
            std::getline(ss, target, '(');

            std::stringstream ss_target(target);
            std::string target_clean;
            std::getline(ss_target, target_clean, ' ');
            def.index_target = target_clean;

            std::string key;
            ss >> std::ws;
            std::getline(ss, key, ')');

            std::stringstream ss_key(key);
            std::string key_clean;
            std::getline(ss_key, key_clean, ' ');
            def.index_key = key_clean;
        }

        else if (lower_string(def.definition_of) == "table") {
            std::string _;
            std::getline(ss, _, '(');
            std::string column_definition;
            std::getline(ss, column_definition, ')');
    
            ss.str(std::string());
            ss << column_definition;
            std::vector<std::string> column;
            std::string token;
            ss >> std::ws;
            while(std::getline(ss, token, ',')) {
                ss >> std::ws;
                column.push_back(token);
            }
            for(auto a: column) {
                std::stringstream c(a);
                std::string column_name;
                std::getline(c, column_name, ' ');
                def.columns.push_back(column_name);
                std::string column_type;
                std::getline(c, column_type, ' ');
                def.type.push_back(column_type);
                std::string adds; 
                std::getline(c, adds);
                def.additional_statement.push_back(adds);
            }
        }

        return def;
    }

}