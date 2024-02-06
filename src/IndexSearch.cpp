#include "Query.h"
#include "Database.h"
#include "Decode.h"

std::vector<size_t> get_row_id_from_index(
    size_t page_offset,
    const uint32_t page_size, 
    const Query::DQLStatement &query,
    const Query::DDLStatement &def
) {
    /*
    INDEX TABLE STRUCTURE

    Index interior page (2):
        Rightmost pointer.
        Cells:
            Index interior cell: (Left child pointer, payload size, payload value) 
            Index interior cell: (....)
            ....

    Index leaf page (10):
        Cells:
            Index leaf cell: (Payload size, payload value)
            Index leaf cell: (.....)
            ......


    Note:
    -Index Interior LCP may points to another index interior page or to index leaf page

    companies.db

    Main Page (Page obtained from master table)
    Index Interior:
        Cells (1):
            LCP to Index interior
                Cells (60):
                    LCP to Index Leaf
                    LCP to Index Leaf
                    ......
                Rightmost pointer:
                    Pointer to Index Leaf:
                        Cells ():


        Rightmost pointer:
            Pointer to Index interior
                Cells (60):
                    LCP to Index Leaf
                    LCP to Index Leaf
                    ......                   


    STEPS

    1. Read query specification, does the where clause points to the index?
    2. Read page type
    3. If page type == 2, 
        Read rightmost pointer
        Read cell count & cell pointers array
        Do binary search on cells, if matches the criteria, go to its left child pointer
    4. If page type == 10
        Read cell count & cell pointers array
        Do binary search on cells, if matches the criteria, push the row number
    */
    std::string target_table = def.index_target;
    std::string index_key = def.index_key;
    std::vector<Query::WhereCondition> conditions = query.condition;

    if (query.table != target_table) {
        std::cout << "query table and index table mismatch" << std::endl;
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

    //read page type
    Database::db->seekg(page_offset, std::ios::beg);
    char buf[1] = {};
    Database::db->read(buf, 1);
    uint16_t page_type = buf[0];

    //read cell count
    Database::db->seekg(page_offset + 3, std::ios::beg);
    char buf[2] = {};
    Database::db->read(buf, 2);
    uint32_t cell_count = Decode::to_uint16_t(buf);

    //read rightmost pointer
    Database::db->seekg(page_offset + 8, std::ios::beg);
    char buff[4] = {};
    Database::db->read(buff, 4);
    uint32_t rightmost_pointer = Decode::to_uint32_t(buff);

    //jump the headers & read the cell pointers array
    size_t cell_offset = page_offset + Database::HeaderSize::INTERIOR;
    std::vector<size_t> cell_offsets;
    for (int i = 0; i < cell_count; ++i) {
        Database::db->seekg(cell_offset, std::ios::beg);
        char num[2] = {};
        Database::db->read(num, 2);
        //absoulte cell offset = page_offset + offset found at cell pointer array
        cell_offsets.push_back(page_offset + Decode::to_uint16_t(num)); 
        cell_offset += 2;
    }

    std::vector<size_t> row_ids;
    if (page_type == Database::INDEX_INTERIOR) {
        //binary search
        size_t start = 0;
        size_t end = cell_offsets.size();
        size_t mid = start+(end-start)/2;



    }
    else if (page_type == Database::INDEX_LEAF) {

    }

}

void print_result_from_row_id() {
    /*
    
    from bunch of row ids, do binary search on table interior page
    do I have to read cell count & cell pointers or we can jump among cells with the help of row id?
    
    */
}