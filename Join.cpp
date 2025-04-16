#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(0, Bucket(disk)); // placeholder
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	
	vector<uint> disk_pages; // Final output, join result

	// Reserve memory page 0 for output
    // Reserve memory page 1 for reading pages from disk
    const uint OUTPUT_MEM_PAGE = 0;
    const uint SCAN_MEM_PAGE = 1;
    Page* output_page = mem->mem_page(OUTPUT_MEM_PAGE);

    // Set hash table size to (MEM_SIZE_IN_PAGE - 2), as required for probe phase.
    const uint hash_table_size = MEM_SIZE_IN_PAGE - 2;

	// Iterate over all buckets from partition phase
	for(int k = 0; k < partitions.size(); k++) {
		Bucket& bucket = partitions[k];

		// Decide which partition is smaller
        // The smaller one will be loaded completely into the hash-table
		bool left_is_smaller = (bucket.num_left_rel_record <= bucket.num_right_rel_record);
		vector<uint> smaller_pages;
		vector<uint> larger_pages;
		if(left_is_smaller) {
			smaller_pages = bucket.get_left_rel();
			larger_pages = bucket.get_right_rel();
		}
		else {
			smaller_pages = bucket.get_right_rel();
			larger_pages = bucket.get_left_rel();
		}

		// Build an in-memory hash table where each slot is a vector of Records.
        vector<vector<Record>> hash_table(hash_table_size);

		// Iterate over all records in the smaller relation
		for(uint disk_page_id : smaller_pages) {
			// Load in records from disk into a page
			mem->loadFromDisk(disk, disk_page_id, SCAN_MEM_PAGE);
			Page* current_page = mem->mem_page(SCAN_MEM_PAGE);

			// Process all records in the page
			for(uint j = 0; j < current_page->size(); j++) {
				Record r = current_page->get_record(j);
				// Compute h2 hash
				uint hash_index = r.probe_hash() % hash_table_size;
				// Put into bucket
				hash_table[hash_index].push_back(r);
			}
		}
		// Iterate over all records in the larger relation, compare to hashed records of smaller relation to find joins
		for(uint disk_page_id : larger_pages) {
			// Load in records from disk into a page
			mem->loadFromDisk(disk, disk_page_id, SCAN_MEM_PAGE);
			Page* current_page = mem->mem_page(SCAN_MEM_PAGE);

			// Process all records in the page
			for(uint j = 0; j < current_page->size(); j++) {
				Record r = current_page->get_record(j);
				// Compute h2 hash
				uint hash_index = r.probe_hash() % hash_table_size;
				// Compare with the records already in this bucket
				for(const Record& r_small : hash_table[hash_index]) {
					if(r == r_small) {
						// Match found, load pair into join output page
						output_page->loadPair(r_small, r);
						// Flush output page to disk if full
						if(output_page->full()) {
							uint out_disk_id = mem->flushToDisk(disk, OUTPUT_MEM_PAGE);
							disk_pages.push_back(out_disk_id);
						}
					}
				}
			}
		}

	}
	// Flush partially filled output page if exists
	if(!output_page->empty()) {
		uint out_disk_id = mem->flushToDisk(disk, OUTPUT_MEM_PAGE);
		disk_pages.push_back(out_disk_id);
	}

	return disk_pages;
}
