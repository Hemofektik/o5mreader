
#include <iostream>
#include <iomanip>
#include <chrono>

#include "o5mreader.h"

using namespace std;
using namespace std::chrono;

int main() 
{
	O5mreader* reader;
	O5mreaderDataset ds;
	O5mreaderIterateRet ret, ret2;
	char *key, *val;
	uint64_t nodeId;
	uint64_t refId;
	uint8_t type;
	char *role;

	FILE* f = fopen("../test/files/netherlands-latest.osm.o5m", "rb");
	o5mreader_open(&reader, f);

	high_resolution_clock::time_point t1 = high_resolution_clock::now();

	uint64_t numDataSetsReadPreviously = 0;
	uint64_t numDataSetsRead = 0;
	while ((ret = o5mreader_iterateDataSet(reader, &ds)) == O5MREADER_ITERATE_RET_NEXT) {
		numDataSetsRead++;

		switch (ds.type) {
			// Data set is node
		case O5MREADER_DS_NODE:

			// Could do something with ds.id, ds.lon, ds.lat here, lon and lat are ints in 1E+7 * degree units
			// Node tags iteration, can be omited
			while ((ret2 = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT) {
				// Could do something with tag key and val
			}
			break;
			// Data set is way
		case O5MREADER_DS_WAY:
			// Could do something with ds.id
			// Nodes iteration, can be omited
			while ((ret2 = o5mreader_iterateNds(reader, &nodeId)) == O5MREADER_ITERATE_RET_NEXT) {
				// Could do something with nodeId
			}
			// Way taga iteration, can be omited
			while ((ret2 = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT) {
				// Could do something with tag key and val
			}
			break;
			// Data set is rel
		case O5MREADER_DS_REL:
			// Could do something with ds.id
			// Refs iteration, can be omited
			while ((ret2 = o5mreader_iterateRefs(reader, &refId, &type, &role)) == O5MREADER_ITERATE_RET_NEXT) {
				// Could do something with refId (way or node or rel id depends on type), type and role
			}
			// Relation tags iteration, can be omited
			while ((ret2 = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT) {
				// Could do something with tag key and val
			}
			break;
		}

		if ((numDataSetsRead & 0x7FFFF) == 0)
		{
			uint64_t numDataSetsReadIntermediate = numDataSetsRead - numDataSetsReadPreviously;
			high_resolution_clock::time_point t2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

			cout << "Num Datasets read per second: " << (int)(numDataSetsReadIntermediate / time_span.count()) << "\r";

			numDataSetsReadPreviously = numDataSetsRead;
			t1 = t2;
		}
	}

	o5mreader_close(reader);
	fclose(f);

	cout << "Num Datasets read: " << numDataSetsRead << "                               " << endl;

	return 0;
 }
