
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <vector>

#include "o5mreader.h"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <spatialindex/SpatialIndex.h>
#include <ZFXMath.h>

#include "MGArchive.h"

using namespace std;
using namespace std::chrono;
using namespace leveldb;
using namespace SpatialIndex;
using namespace ZFXMath;


typedef TFixed<int32_t, 7> O5MCoord;

struct NodeValue
{
	O5MCoord lon;
	O5MCoord lat;
	uint64_t fileOffset;
	uint64_t readerOffset;
};


static int numDBReadNodes = 1;
static high_resolution_clock::time_point readNodeFromDB_T1;


struct BBox
{
	O5MCoord minX;
	O5MCoord minY;
	O5MCoord maxX;
	O5MCoord maxY;
};

class Tag
{
public:
	string key;
	string value;
};

class Way
{
public:
	uint64_t id;
	BBox bbox;
	TPolygon2D<TFixed<int32_t, 7>> polygon;
	vector<Tag> tags;
};


MGArchive& operator<<(MGArchive& archive, BBox& bbox)
{
	archive << bbox.minX << bbox.minY << bbox.maxX << bbox.maxY;
	return archive;
}

MGArchive& operator<<(MGArchive& archive, Tag& tag)
{
	archive << tag.key << tag.value;
	return archive;
}

template<typename T>
MGArchive& operator<<(MGArchive& archive, TPolygon2D<T>& polygon)
{
	if (archive.IsSaving())
	{
		uint64_t numVertices = polygon.GetNumVertices();
		archive << numVertices;
		archive.Serialize((const char*)polygon.GetVertices(), numVertices * sizeof(TVector2D<T>));
	}
	else
	{
		uint64_t numVertices;
		archive << numVertices;
		polygon.SetNumVertices((int)numVertices);
		archive.Serialize((char*)polygon.GetVertices(), numVertices * sizeof(TVector2D<T>));
	}

	return archive;
}

template<typename T>
MGArchive& operator<<(MGArchive& archive, vector<T>& v)
{
	uint64_t numElements = v.size();
	archive << numElements;

	if (archive.IsLoading())
	{
		v.resize(numElements);
	}

	for (size_t i = 0; i < numElements; i++)
	{
		archive << v[i];
	}

	return archive;
}

MGArchive& operator<<(MGArchive& archive, Way& way)
{
	archive << way.id << way.bbox << way.polygon << way.tags;
	return archive;
}


#pragma optimize( "", off )
void ReadWay(O5mreader* reader, DB *db, const uint64_t& wayId)
{
	ReadOptions ro;
	uint64_t nodeId;
	O5mreaderIterateRet ret;

	Way way;
	way.id = wayId;

	BBox bb;
	bb.minX.value = numeric_limits<int32_t>::max();
	bb.minY.value = numeric_limits<int32_t>::max();
	bb.maxX.value = numeric_limits<int32_t>::min();
	bb.maxY.value = numeric_limits<int32_t>::min();

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

				bb.minX = Min(bb.minX, node.lon);
				bb.minY = Min(bb.minY, node.lat);
				bb.maxX = Max(bb.maxX, node.lon);
				bb.maxY = Max(bb.maxY, node.lat);

				TVector2D<O5MCoord> nodeLocation(node.lon, node.lat);
				way.polygon.AddVertex(nodeLocation);
			}
		}
		numDBReadNodes++;
	}

	way.bbox = bb;

	char *key, *val;
	while ((ret = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT) 
	{
		Tag tag;
		tag.key = key;
		tag.value = val;
		way.tags.push_back(tag);
	}

	MGArchive archive;
	archive << way;

	uint64_t waySize = 0;
	char* serializedWay = archive.ToByteStream(waySize);
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
				nv.lon.value = ds.lon;
				nv.lat.value = ds.lat;
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
				ReadWay(reader, db, ds.id);
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
