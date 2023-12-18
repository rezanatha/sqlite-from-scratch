#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <memory>
#include <sstream>
#include <algorithm>
#include "Decode.h"
#include "Database.h"
#include "Query.h"

std::string lower_string (std::string s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
    [](unsigned char c){ return std::tolower(c); });
    return result;
}

std::vector<std::string> split_string (const std::string &str, const char delimiter) {
    std::vector<std::string> output;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, delimiter) ) {
        output.push_back(token);
    }
    return output;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];
    
    std::shared_ptr<std::ifstream> db_object (new std::ifstream(database_file_path, std::ios::binary));
    Database::db = db_object.get();


    if (!Database::db) {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }
    
    uint32_t page_size = Database::db_header_page_size();
    uint32_t num_page = Database::db_header_num_of_pages();
    uint32_t encoding = Database::db_header_encoding();

    uint16_t cell_count = Database::master_header_cell_count();

    // Process master table
    std::vector<Database::Row> master_table = Database::read_master_table();
    std::map<std::string, Database::Row*> table_map;

    for (auto &row: master_table) {
        std::string table_name = *static_cast<std::string*>(row.field[2].field_value);
        //uint16_t root_page_num = *static_cast<uint16_t*>(row.field[3].field_value);
        table_map[table_name] = &row;
    }  

    //Process command
    if (command == ".dbinfo") {
        printf("database page size: %u \n", page_size);
        //printf("number of pages: %u \n", num_page);
        printf("number of tables: %u \n", cell_count); 
        //printf("encoding: %u \n", encoding); 

    } else if (command == ".tables") {
        for (auto r: master_table) {
            //0 = 
            printf("row size %zu, row_id %d ", r.row_size, r.row_id);
            uint32_t field_type = r.field[4].field_type;
            int field_size = r.field[4].field_size;
            std::string s = *static_cast<std::string*>(r.field[4].field_value);
            //printf("%s \n", s.c_str());
            std::cout << field_type << " " << field_size << std::endl;
            std::cout << std::endl;
        }

    } else {
        Query::DQLStatement query = Query::parse_query(command);
        //std::cout << query.columns[0] << std::endl;
        //std::cout << query.table << std::endl;

        Query::process_select_from_statement(query, table_map, page_size);

        // std::vector<std::string> c = split_string(command, ' '); 
        // if (lower_string(c[1]) == "count(*)") {
        //     std::string table = c.back();
        //     uint16_t root_page = root_page_map[table];
        //     Database::db->seekg((root_page - 1) * page_size + 3, std::ios::beg);
        //     char buf[2] = {};
        //     Database::db->read(buf, 2);
        //     uint16_t cell_count = Decode::to_uint16_t(buf);
        //     printf("%u\n", cell_count);
        // }

        /* homework
        the goal is to read sql query so it outputs desired result

        SELECT t1, t2, t3 FROM table WHERE t4 = 10
               ^^^^^^^^^^      ^^^^^      ^^^^^^^^^
                columns     target table   filter
            (comma-separated)
        
        1. Design function to read query and return a query object. Split the query like above.
        2. Design function to parse table definition so we can locate at which field of a row a column is
        3. We already know the root page number, we can locate the table in the binary and read it just like we read our master table
        3. Design function to print the result as desired, table-like

        
        */
    }
    return 0;
}
