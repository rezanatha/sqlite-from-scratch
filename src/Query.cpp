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
        //std::cout << "Cols: " << cols << std::endl;
        offset = end;
        std::stringstream ss_cols(cols);
        std::string token;
        while (std::getline(ss_cols, token, ',')) {
            size_t start = token.find_first_not_of(' ');
            size_t end = token.find_last_not_of(' ');
            token = (start == std::string::npos) ? "": token.substr(start, end-start+1);
            columns.push_back(token);
            //std::cout << "col: " << token << std::endl;
        }
        return columns;
    }

    std::string get_table_name (const std::string &query, size_t &offset) {
        //std::cout << "get_table_name offset: " << offset << std::endl;
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

    void process_select_from_statement(
         const DQLStatement &query, 
         const std::map<std::string, Database::Row*> &table_map,
         uint32_t page_size
    ) {
        auto master_table_row = table_map.at(query.table);
        std::string table_name = *static_cast<std::string*>(master_table_row->field[2].field_value);
        std::string table_definition = *static_cast<std::string*>(master_table_row->field[4].field_value);
        uint16_t root_page = *static_cast<uint16_t*>(master_table_row->field[3].field_value);
        uint16_t cell_offset = (root_page - 1) * page_size;

        //read cell count
        Database::db->seekg(cell_offset + 3, std::ios::beg);
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
            2. Get cell offsets for all of our rows. Note: cell offsets are smaller than our table's header. CHatgpt says this is totally possible.
            3. Get row values for all of our rows
            4. 
            */
            //std::cout << "offset: " << cell_offset << std::endl;
            Database::db->seekg(cell_offset, std::ios::beg);
            char buf[1] = {};
            Database::db->read(buf, 1);
            uint16_t page_type = buf[0];
            std::cout << "page_type: " << page_type << std::endl;

            if (page_type == 0x0d) {
                cell_offset += Database::LEAF_PAGE_HEADER_SIZE;
            }
            else if (page_type == 0x05) {
                cell_offset += Database::INTERIOR_PAGE_HEADER_SIZE;
            }
            else {
                throw std::logic_error("unknown page type");
            }

            //std::cout << "offset: " << cell_offset << std::endl;
            std::vector<uint16_t> cell_offsets;
            for (int i = 0; i < cell_count; ++i) {
                //std::cout << "cell offset: " << cell_offset << std::endl;
                Database::db->seekg(cell_offset, std::ios::beg);
                char buf[2] = {};
                Database::db->read(buf, 2);
                //printf("%d %d \n", buf[0], buf[1]);
                cell_offsets.push_back(Decode::to_uint16_t(buf));
                cell_offset += 2;
            }
            // for(auto &a: cell_offsets) {
            //     std::cout << a << std::endl;
            // }

            std::vector<Database::Row> table;
            for (auto &offset: cell_offsets) {
                std::cout << "cell offset: " << offset << std::endl;
                uint16_t row_id_offset = offset, payload_offset = offset;

                uint16_t new_offset = offset;
                Database::Row row;
                row.row_size = Decode::read_varint(Database::db, offset, row_id_offset);
                row.row_id = Decode::read_varint(Database::db, row_id_offset, payload_offset);

                uint64_t row_id_new = Decode::read_varint_new(Database::db, row_id_offset);
                std::cout << "row_id_new " << row_id_new << std::endl;

                std::cout << "row size " << row.row_size << " row id " << row.row_id << " payload offset: " << payload_offset << std::endl;
                row.field = Database::read_row_debug(payload_offset);
                table.push_back(row);
            }

        }

    }

}
