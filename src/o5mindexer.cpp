
#include <iostream>
#include <iomanip>
#include <chrono>

#include "o5mreader.h"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

using namespace std;
using namespace std::chrono;
using namespace leveldb;


struct NodeValue
{
	int32_t lon;
	int32_t lat;
	uint64_t fileOffset;
	uint64_t readerOffset;
};


static int numDBReadNodes = 1;
static high_resolution_clock::time_point readNodeFromDB_T1;

#pragma optimize( "", off )
void ReadWay(O5mreader* reader, DB *db)
{
	ReadOptions ro;
	uint64_t nodeId;
	O5mreaderIterateRet ret;

	if (readNodeFromDB_T1 == high_resolution_clock::time_point::time_point())
	{
		readNodeFromDB_T1 = high_resolution_clock::now();
	}

	while ((ret = o5mreader_iterateNds(reader, &nodeId)) == O5MREADER_ITERATE_RET_NEXT) 
	{
		string value;
		auto status = db->Get(ro, Slice((const char*)&nodeId, sizeof(uint64_t)), &value);
		if (status.ok())
		{
			if (sizeof(NodeValue) == value.length())
			{
				NodeValue node;
				memcpy(&node, value.c_str(), sizeof(NodeValue));

				node.lon++;
			}
		}
		numDBReadNodes++;
	}

	char *key, *val;
	while ((ret = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT) {
		// Could do something with tag key and val
	}
}

#pragma optimize( "", on )
int main() 
{
	O5mreader* reader;
	O5mreaderDataset ds;
	O5mreaderIterateRet ret, ret2;
	char *key, *val;
	uint64_t refId;
	uint8_t type;
	char *role;

	string nodeDBFilePath = "../test/files/antarctica-2016-01-06.osm.o5m.nd-idx";
	

	DB *db;
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 100 << 20;
	DB::Open(options, nodeDBFilePath, &db);




	FILE* f = fopen("../test/files/antarctica-2016-01-06.osm.o5m", "rb");
	
	o5mreader_open(&reader, f);

	WriteBatch wb;
	WriteOptions wo;

	high_resolution_clock::time_point t1 = high_resolution_clock::now();

	uint64_t numDataSetsReadPreviously = 0;
	uint64_t numDataSetsRead = 0;
	while ((ret = o5mreader_iterateDataSet(reader, &ds)) == O5MREADER_ITERATE_RET_NEXT) 
	{
		numDataSetsRead++;

		switch (ds.type) 
		{
			case O5MREADER_DS_NODE:
			{
				if (ds.isEmpty) break;

				NodeValue nv;
				nv.lon = ds.lon;
				nv.lat = ds.lat;
				nv.fileOffset = reader->f->fOffset;
				nv.readerOffset = reader->offset;

				wb.Put(
					Slice((const char*)&ds.id, sizeof(uint64_t)),
					Slice((const char*)&nv, sizeof(NodeValue)));

				// Could do something with ds.id, ds.lon, ds.lat here, lon and lat are ints in 1E+7 * degree units
				// Node tags iteration, can be omited
				/*while ((ret2 = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT)
				{
					if (key == val)
					{

					}
					// Could do something with tag key and val
				}*/
				break;
			}
			case O5MREADER_DS_WAY:
				ReadWay(reader, db);
				break;
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

		if ((numDBReadNodes & 0xFF) == 0)
		{
			uint64_t numNodesReadIntermediate = 0xFF;
			high_resolution_clock::time_point readNodeFromDB_T2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(readNodeFromDB_T2 - readNodeFromDB_T1);
			
			cout << "Num Nodes read per second: " << (int)(numNodesReadIntermediate / time_span.count()) << "                         \r";

			readNodeFromDB_T1 = readNodeFromDB_T2;
		}

		if ((numDataSetsRead & 0x7FFFF) == 0)
		{
			db->Write(wo, &wb);
			wb.Clear();

			uint64_t numDataSetsReadIntermediate = numDataSetsRead - numDataSetsReadPreviously;
			high_resolution_clock::time_point t2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

			cout << "Num Datasets read per second: " << (int)(numDataSetsReadIntermediate / time_span.count()) << "                               \r";

			numDataSetsReadPreviously = numDataSetsRead;
			t1 = t2;
		}
	}

	db->Write(wo, &wb);
	wb.Clear();

	delete db;

	o5mreader_close(reader);
	fclose(f);

	cout << "Num Nodes read: " << numDBReadNodes << "                               " << endl;
	cout << "Num Datasets read: " << numDataSetsRead << "                               " << endl;

	return 0;
 }
