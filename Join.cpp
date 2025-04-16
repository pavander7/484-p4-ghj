#include "Join.hpp"
#include <iostream>

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// STEP ONE: Setup
    const uint num_buckets = MEM_SIZE_IN_PAGE - 1;

    // // retrieve scratch page
    // Page *scratch_page = mem->mem_page(num_buckets);  // this is arbitrary (only need one page)
    // if (!scratch_page->empty()) {
    //     scratch_page->reset();
    // }

    // initialize output vector
    std::vector<Bucket> partitions;
    partitions.reserve(num_buckets);

    // initialize buckets
    for (size_t b = 0; b < num_buckets; b++) {
        partitions.emplace_back(disk);
    }

    // STEP TWO: Hash left_rel

    // loop through page_ids in left_rel
    for(uint rel_page = left_rel.first; rel_page < left_rel.second; rel_page++) {
        //cout << "Loading page " << rel_page << ": ";
        mem->loadFromDisk(disk, rel_page, num_buckets);  // load page
		Page* scratch_page = mem->mem_page(num_buckets);
        //cout << scratch_page->size() << " records found\n";

        // loop through record_ids in rel_page
        for(uint record_id = 0; record_id < scratch_page->size(); record_id++) {
            Record record = scratch_page->get_record(record_id);  // load record
            uint h1 = record.partition_hash() % num_buckets;  // hash record
			Page* buffer = mem->mem_page(h1);
			buffer->loadRecord(record);

			if(buffer->full()) {
				uint out_disk_page = mem->flushToDisk(disk, h1);
				partitions[h1].add_left_rel_page(out_disk_page);  // add record to bucket
			}
        }
        scratch_page->reset();  // reset scratch page
    }
	// Flush partially filled buckets to disk
	for (uint b = 0; b < num_buckets; ++b) {
        Page* bucket = mem->mem_page(b);
        if (!bucket->empty()) {
            uint out_dp = mem->flushToDisk(disk, b);
            partitions[b].add_left_rel_page(out_dp);
        }
    }

    // STEP THREE: Hash right_rel

    // loop through page_ids in right_rel
    for(uint rel_page = right_rel.first; rel_page < right_rel.second; rel_page++) {
        //cout << "Loading page " << rel_page << ": ";
        mem->loadFromDisk(disk, rel_page, num_buckets);  // load page
		Page* scratch_page = mem->mem_page(num_buckets);
        //cout << scratch_page->size() << " records found\n";

        // loop through record_ids in rel_page
        for(uint record_id = 0; record_id < scratch_page->size(); record_id++) {
            Record record = scratch_page->get_record(record_id);  // load record
            uint h1 = record.partition_hash() % num_buckets;  // hash record
			Page* buffer = mem->mem_page(h1);
			buffer->loadRecord(record);

			if(buffer->full()) {
				uint out_disk_page = mem->flushToDisk(disk, h1);
				partitions[h1].add_right_rel_page(out_disk_page);  // add record to bucket
			}
        }

        scratch_page->reset();  // reset scratch page
    }
	// Flush partially filled buckets to disk
	for (uint b = 0; b < num_buckets; ++b) {
        Page* bucket = mem->mem_page(b);
        if (!bucket->empty()) {
            uint out_dp = mem->flushToDisk(disk, b);
            partitions[b].add_right_rel_page(out_dp);
        }
    }

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
	for(uint k = 0; k < partitions.size(); k++) {
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

		// Iterate over all records in the smaller partition
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
		// Iterate over all records in the larger partition, compare to hashed records of smaller relation to find joins
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
