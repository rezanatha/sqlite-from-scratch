#include "Query.h"
#include "Database.h"
#include "Decode.h"

namespace Query {
    std::string lower_string (std::string s) {
        std::string result = s;
        std::transform(result.begin(), result.end(), result.begin(),
        [](unsigned char c){ return std::tolower(c); });
        return result;
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
        std::string cols;
        std::string line;
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

    DDLStatement parse_table_definition (
        const std::string &table_name,
        const std::string &table_definition) {

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

    void print_query_result (const DQLStatement &query, const DDLStatement &table_def, std::vector<Database::Cell> &table) {
        std::map<std::string, size_t> map_column_position;
        for(size_t i = 0; i < table_def.columns.size(); ++i) {
            map_column_position[table_def.columns[i]] = i; 
            //std::cout << def.columns[i] << ";" << std::endl;
        }
        
        //output table entries according to query (No filter, no limit)
        for (auto &row: table) {
            size_t column_length = query.columns.size();
            std::vector<std::string> columns = query.columns;
            for (size_t i = 0; i < column_length; ++i) {
                Database::RowField field = row.field[map_column_position[columns[i]]-1];
                std::cout << *static_cast<std::string*>(field.field_value);
                if(column_length > 1 && i != column_length-1) {
                    std::cout << '|';
                }
            }
            std::cout << std::endl;
        }
    }

    void process_select_from_statement(
         const DQLStatement &query, 
         const std::map<std::string, Database::Cell*> &table_map,
         uint32_t page_size
    ) {
        auto master_table_row = table_map.at(query.table);

        //parse table definition
        std::string table_name = *static_cast<std::string*>(master_table_row->field[2].field_value);
        std::string table_definition = *static_cast<std::string*>(master_table_row->field[4].field_value);
        DDLStatement def = parse_table_definition(table_name, table_definition);

        //get page offset
        uint16_t root_page = *static_cast<uint16_t*>(master_table_row->field[3].field_value);
        const uint16_t page_offset = (root_page - 1) * page_size;

        //read cell count
        Database::db->seekg(page_offset + 3, std::ios::beg);
        char buf[2] = {};
        Database::db->read(buf, 2);
        uint16_t cell_count = Decode::to_uint16_t(buf);
        
        if (query.columns.size() == 1 && query.columns[0] == "count(*)") {
            printf("%u\n", cell_count);
            return;
        }
        else if (query.columns.size() > 0) {
            /*
            1. Read page type
            2. Get cell offsets for all of our rows. Note: cell offsets are smaller than our table's header. 
               Actually you have to add page offset to the cell offsets.
            3. Get row values for all of our rows
            4. Map queried column to the table definition
               SELECT column3, column1 FROM table
               column3 is 3rd in table definition, so we went to the 3rd payload to get column3 and place it as 1st column for our query
               column2 is 2nd in table definition, so we went to the 2nd payload to get column2 and place it as 2nd column for our query
            */
            Database::db->seekg(page_offset, std::ios::beg);
            char buf[1] = {};
            Database::db->read(buf, 1);
            uint16_t page_type = buf[0];
            //std::cout << "page_type: " << page_type << std::endl;
            uint16_t cell_offset = page_offset;
            if (page_type == 0x0d) {
                cell_offset += Database::LEAF_PAGE_HEADER_SIZE;
            }
            else if (page_type == 0x05) {
                cell_offset += Database::INTERIOR_PAGE_HEADER_SIZE;
            }
            else {
                throw std::logic_error("unknown page type");
            }
            std::vector<uint16_t> cell_offsets;
            
            for (int i = 0; i < cell_count; ++i) {
                Database::db->seekg(cell_offset, std::ios::beg);
                char num[2] = {};
                Database::db->read(num, 2);
                //absoulte cell offset = page_offset + offset found at cell pointer array
                cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 
                cell_offset += 2;
            }
            //read cells
            std::vector<Database::Cell> table;
            for (auto &offset: cell_offsets) {
                uint16_t row_id_offset = offset, payload_offset = offset;

                uint16_t new_offset = offset;
                Database::Cell cell;
                cell.payload_size = Decode::read_varint(Database::db, offset, row_id_offset);
                cell.rowid = Decode::read_varint(Database::db, row_id_offset, payload_offset);

                //std::cout << "total number bytes of a payload " << cell.payload_size << " rowid " << cell.rowid << " payload offset: " << payload_offset << std::endl;
                cell.field = Database::read_row(payload_offset);
                table.push_back(cell);
            }

            //print table (debug only)
            // for (auto &row: table) {
            //     for (auto &column: row.field) {
            //         std::cout << *static_cast<std::string*>(column.field_value) << '|';
            //     }
            //     std::cout << std::endl;
            // }            
            print_query_result(query, def, table);
        }

    }

}
