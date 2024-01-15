#include "Query.h"
#include "Database.h"
#include "Decode.h"

namespace Query {
    std::vector<Database::TableLeafCell> read_table_leaf_cell (size_t page_offset) {
        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buf[2] = {};
        Database::db->read(buf, 2);
        uint16_t cell_count = Decode::to_uint16_t(buf);

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::LEAF;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            Database::db->seekg(cell_offset, std::ios::beg);
            char num[2] = {};
            Database::db->read(num, 2);
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 
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
            cell.payload_size = Decode::read_varint(Database::db, offset, row_id_offset);
            cell.rowid = Decode::read_varint(Database::db, row_id_offset, payload_offset);
            cell.field = Database::read_row(payload_offset);
            table.push_back(cell);
        }

        return table;

    }

    std::vector<Database::TableInteriorCell> read_table_interior_cell (size_t page_offset)  {
        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buf[2] = {};
        Database::db->read(buf, 2);
        uint16_t cell_count = Decode::to_uint16_t(buf);

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::INTERIOR;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            Database::db->seekg(cell_offset, std::ios::beg);
            char num[2] = {};
            Database::db->read(num, 2);
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 
            cell_offset += 2;
        }

        //read cells for interior table page
        //if our page type is interior table, then a cell contains of:
        //4-byte left child pointer
        //varint row ID
        std::vector<Database::TableInteriorCell> table;
        for (auto &offset: cell_offsets) {
            Database::db->seekg(offset, std::ios::beg);
            char num[4] = {};
            Database::db->read(num, 4);
            uint32_t left_child_pointer = Decode::to_uint32_t(num);
            size_t rowid_offset = offset + 4;
            size_t _;
            uint32_t rowid = Decode::read_varint(Database::db, rowid_offset, _);
            Database::TableInteriorCell cell = {.left_child_pointer = left_child_pointer, .rowid = rowid};
            table.push_back(cell);
        }

        return table;

    }

    Database::IndexInteriorPage read_index_interior_page (size_t page_offset) {
        Database::IndexInteriorPage page;
        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buf[2] = {};
        Database::db->read(buf, 2);
        uint16_t cell_count = Decode::to_uint16_t(buf);
        page.cell_count = cell_count;

        //read rightmost pointer
        Database::db->seekg(page_offset + 8, std::ios::beg);
        char buff[4] = {};
        Database::db->read(buff, 4);
        uint32_t rightmost_pointer = Decode::to_uint32_t(buff);
        page.rightmost_pointer = rightmost_pointer;

        std::cout << "(2) cell count: " << cell_count << " rightmost pointer: " << rightmost_pointer << std::endl;

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::INTERIOR;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            Database::db->seekg(cell_offset, std::ios::beg);
            char num[2] = {};
            Database::db->read(num, 2);
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 
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
            Database::db->seekg(offset, std::ios::beg);
            char num[4] = {};
            Database::db->read(num, 4);
            uint32_t left_child_pointer = Decode::to_uint32_t(num);
            size_t rowid_offset = offset + 4;

            //std::cout << "lcp: " << left_child_pointer << std::endl;

            size_t payload_offset;
            uint32_t payload_size = Decode::read_varint(Database::db, rowid_offset, payload_offset);

            //std::cout << "offsets: " << rowid_offset << " " << payload_offset << std::endl;

            std::vector<Database::RowField> field = Database::read_row(payload_offset);

            //read overflow page

            Database::IndexInteriorCell cell = {
                .left_child_pointer = left_child_pointer, 
                .payload_size = payload_size,
                .field = field
            };
            cells.push_back(cell);
        }
        page.cells = cells;

        return page;
    }

    std::vector<Database::IndexLeafCell> read_index_leaf_cell (size_t page_offset) {
        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buf[2] = {};
        Database::db->read(buf, 2);
        uint16_t cell_count = Decode::to_uint16_t(buf);

        //jump the headers
        size_t cell_offset = page_offset + Database::HeaderSize::LEAF;

        //read the cell pointers array
        std::vector<size_t> cell_offsets;
        for (int i = 0; i < cell_count; ++i) {
            Database::db->seekg(cell_offset, std::ios::beg);
            char num[2] = {};
            Database::db->read(num, 2);
            //absoulte cell offset = page_offset + offset found at cell pointer array
            cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 

            //std::cout << "cell pointers: " <<  page_offset + Decode::to_uint16_t(num) << std::endl;
            cell_offset += 2;
        }

        //read index leaf cell
        // 1. payload size (in bytes)
        // 2. payload
        // 3. 4 bytes first_overflow_page
        std::vector<Database::IndexLeafCell> table;
        for (auto &offset: cell_offsets) {
            size_t payload_offset;
            uint32_t payload_size = Decode::read_varint(Database::db, offset, payload_offset);
            //std::cout << "offsets: " << rowid_offset << " " << payload_offset << std::endl;
            std::vector<Database::RowField> field = Database::read_row(payload_offset);
            Database::IndexLeafCell cell = {
                .payload_size = payload_size,
                .field = field
            };
            table.push_back(cell);
        }
        return table;
    }

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

        return def;
    }

    void print_query_result (
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
        
        // for (auto &a: map_column_position) {
        //     std::cout << a.first << ' ' << a.second << std::endl;
        // }
        
        //output table entries according to query 
        std::vector<std::string> columns = query.columns;
        std::vector<WhereCondition> conditions = query.condition;

        if (query.columns.size() == 1 && query.columns[0] == "*") { //select all columns
            columns = table_def.columns;
        }
        int counter = 0;
        for (auto &row: table) {
            //std::cout << "size: " << row.field.size() << std::endl;
            // for (auto f: row.field) {
            //     std::cout << f.field_type << " - " << f.field_size << " - ";
            //     if (f.field_type == Database::SerialType::SERIAL_NULL) {
            //         std::cout << "NULL" << '|';
            //     }
            //     else {
            //         std::cout << *static_cast<std::string*>(f.field_value) << '|';
            //     }
            // }
            // std::cout << std::endl;

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

    }

    void process_table_interior (
        std::vector<Database::TableInteriorCell> &table, 
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
        ) {
        for (auto &c: table) {
            
            const size_t page_offset = (c.left_child_pointer - 1) * page_size;
            std::cout << "table interior left child pointer: " << c.left_child_pointer << " page offset: " << page_offset << " rowid: " << c.rowid << std::endl;
            Database::db->seekg(page_offset, std::ios::beg);
            char buf[1] = {};
            Database::db->read(buf, 1);
            uint16_t page_type = buf[0];
            //std::cout << " page_type: " << page_type << std::endl;
            if (page_type == Database::PageType::TABLE_LEAF) {
                std::vector<Database::TableLeafCell> leaf_table = read_table_leaf_cell(page_offset);
                print_query_result(query, def, leaf_table, c.rowid);

            } else if (page_type == Database::PageType::TABLE_INTERIOR) {
                std::vector<Database::TableInteriorCell> interior_table = read_table_interior_cell(page_offset);
                process_table_interior(interior_table, page_size, query, def);
            }
        }
    }

    void process_index_leaf (
        std::vector<Database::IndexLeafCell> &table,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    ) {
        //std::cout << "cell size: " << table.size() << std::endl;
        for(auto cell: table) {
            char* id = static_cast<char*>(cell.field[1].field_value);

            if (*static_cast<std:: string*>(cell.field[0].field_value) == "tonga") {
                std::cout 
                << " field size: " << cell.field.size()
                << " country: " << *static_cast<std:: string*>(cell.field[0].field_value) 
                //<< " page id: " << Decode::deserialize_24_bit_to_unsigned(id)
                << " page id 8 bit: " << static_cast<uint16_t>(id[0])
                << " page id type: " << cell.field[1].field_size
                << std::endl; 
            }      
        }
    }

    void process_index_interior (
        Database::IndexInteriorPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    
    ) {
        std::cout << "process_index_interior() cell size: " << page.cells.size() << std::endl;
        for(auto cell: page.cells) {
            const size_t page_offset = (cell.left_child_pointer - 1) * page_size;
            std::cout << "lcp: " << cell.left_child_pointer << " offset: "<< page_offset << std::endl;

            Database::db->seekg(page_offset, std::ios::beg);
            char buf[1] = {};
            Database::db->read(buf, 1);
            uint16_t page_type = buf[0];

            //char* id = static_cast<char*>(cell.field[1].field_value);
            // std::cout 
            // << "page_type: " << page_type 
            // << " field size: " << cell.field.size()
            // << " country: " << *static_cast<std:: string*>(cell.field[0].field_value) 
            // << " page id: " << Decode::deserialize_24_bit_to_unsigned(id)
            // //<< " page id 8 bit: " << static_cast<uint16_t>(id[0])
            // << " page id serial type: " << cell.field[1].field_type
            // << std::endl;

            if (page_type == Database::PageType::INDEX_INTERIOR) {
                Database::IndexInteriorPage index_interior = read_index_interior_page(page_offset);
                process_index_interior(index_interior, page_size, query, def);
            }
            else if (page_type == Database::PageType::INDEX_LEAF) {
                std::vector<Database::IndexLeafCell> index_leaf = read_index_leaf_cell(page_offset);
                process_index_leaf(index_leaf, page_size, query, def);

            }
        }

        //process rightmost pointer
        const size_t page_offset = (page.rightmost_pointer - 1) * page_size;
        std::cout << "rmp: " << page.rightmost_pointer << " offset: "<< page_offset << std::endl;

        Database::db->seekg(page_offset, std::ios::beg);
        char buf[1] = {};
        Database::db->read(buf, 1);
        uint16_t page_type = buf[0];

        if (page_type == Database::PageType::INDEX_INTERIOR) {
                Database::IndexInteriorPage index_interior = read_index_interior_page(page_offset);
                process_index_interior(index_interior, page_size, query, def);
        }
        else if (page_type == Database::PageType::INDEX_LEAF) {
            std::vector<Database::IndexLeafCell> index_leaf = read_index_leaf_cell(page_offset);
            process_index_leaf(index_leaf, page_size, query, def);
        }

    }

    void process_select_from_statement(
         const DQLStatement &query, 
         const std::map<std::string, std::vector<Database::PageHeader>> &table_map,
         uint32_t page_size
    ) {
        for (auto table: table_map.at(query.table)) {
            //parse table definition
            std::string table_name = table.name;
            std::string table_definition = table.definition;
            DDLStatement def = parse_table_definition(table_name, table_definition);
    
            //get page offset
            const size_t page_offset = table.page_offset;
    
            //read page types
            uint16_t page_type = table.page_type;
    
            //read cell count
            uint16_t cell_count = table.cell_count;
    
            std::cout << "table: " << table_name << " cell count: " << cell_count << " page type: " << page_type << std::endl;
    
            if (page_type == Database::PageType::TABLE_LEAF 
            && query.columns.size() == 1 
            && query.columns[0] == "count(*)"
            ) {
                printf("%u\n", cell_count);
                return;
            }
            else if (query.columns.size() > 0) {
                if (page_type == Database::PageType::TABLE_LEAF) {
                    std::vector<Database::TableLeafCell> table = read_table_leaf_cell(page_offset);
                    print_query_result(query, def, table, 0);
    
                } else if (page_type == Database::PageType::TABLE_INTERIOR) { 
                    // std::vector<Database::TableInteriorCell> table = read_table_interior_cell(page_offset);
                    // process_table_interior(table, page_size, query, def);
    
                } else if (page_type == Database::PageType::INDEX_INTERIOR) { 
                    //std::cout << "READ INTERIOR INDEX" << std::endl;
                    Database::IndexInteriorPage page = read_index_interior_page(page_offset);
                    process_index_interior(page, page_size, query, def);
                }
            }
        }
    }


}

/* read leaf table page, page type = 13 (I)
1. skip the headers (8 bytes)
2. read the cell pointers array
3. read the cells in leaf table cells format
4. print query result

read interior table page, page type = 10 (II)
1. skip the headers (12 bytes)
2. read the cell pointers array
3. read the cells in interior table cells format
4. get the absolute page offset and page type
5. if page_type = 13, goto (I), if 10 goto (II)
*/
