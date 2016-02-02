
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <vector>
#include <filesystem>

#include "o5mreader.h"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/comparator.h>
#include <leveldb/filter_policy.h>

#include <spatialindex/SpatialIndex.h>

#include <ZFXMath.h>

#include "MGArchive.h"
#include "o5mindexer.h"

using namespace std;
using namespace std::chrono;
using namespace leveldb;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
using namespace ZFXMath;



typedef int32_t O5MCoord;

struct NodeValue
{
	O5MCoord lon;
	O5MCoord lat;
	uint64_t fileOffset;
	uint64_t readerOffset;
};


static int numDBReadNodes = 0;
static int numDBReadNodesPreviously = 0;
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
	TPolygon2D<int32_t> polygon;
	vector<Tag> tags;
};

//#pragma optimize( "", off )

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


Slice IdToSlice(const uint64_t& id)
{
	Slice slice((const char*)&id, sizeof(uint64_t));
	return slice;
}

uint64_t SliceToId(const Slice slice)
{
	uint64_t* id = (uint64_t*)slice.data();
	return *id;
}

class OSMIdComparator : public Comparator {
public:
	virtual int Compare(const Slice& a, const Slice& b) const
	{
		uint64_t* idA = (uint64_t*)a.data();
		uint64_t* idB = (uint64_t*)b.data();
		int64_t delta = (*idA - *idB);
		if (delta == 0) return 0;
		return delta < 0 ? -1 : 1;
	}

	virtual const char* Name() const
	{
		return "OSMIdComparator";
	}

	virtual void FindShortestSeparator( std::string* start, const Slice& limit) const {};
	virtual void FindShortSuccessor(std::string* key) const {};
};



class CachedDiskStorageManager : public SpatialIndex::IStorageManager
{
public:
	CachedDiskStorageManager(const string& filePath)
		: db(NULL)
		, wasChanged(false)
		, nextPage(0)
		, currentUseIndex(0)
	{
		options.comparator = new OSMIdComparator();
		options.create_if_missing = true;
		options.write_buffer_size = 10 << 20;
		options.filter_policy = NewBloomFilterPolicy(32);
		DB::Open(options, filePath, &db);
	}

	virtual ~CachedDiskStorageManager() 
	{
		flush();

		delete options.comparator;
		delete db;
		delete options.filter_policy;
	}

	virtual void flush() 
	{
		if (wasChanged)
		{
			FlushLRUCache(pageIndex.size());

			db->CompactRange(NULL, NULL);
			wasChanged = false;
		}

		for (auto it = pageIndex.begin(); it != pageIndex.end(); ++it) delete (*it).second;
	}

	virtual void loadByteArray(const id_type page, uint32_t& len, byte** data) 
	{
		*data = NULL;
		len = 0;

		std::map<id_type, Entry*>::iterator it = pageIndex.find(page);
		if (it == pageIndex.end())
		{
			ReadOptions ro;
			string result;
			if (db->Get(ro, IdToSlice(page), &result).ok())
			{
				len = (uint32_t)result.size();
				*data = new byte[len];
				memcpy(*data, result.data(), len);
				AddEntry(page, len, (const byte*)result.data());
			}
			else
			{
				throw InvalidPageException(page);
			}
		}
		else
		{
			it->second->useIndex = currentUseIndex;
			len = it->second->length;
			*data = new byte[len];
			memcpy(*data, it->second->data, len);
		}

		currentUseIndex++;
	}

	virtual void storeByteArray(id_type& page, const uint32_t len, const byte* const data) 
	{
		wasChanged = true;

		if (page == StorageManager::NewPage)
		{
			page = nextPage;
			nextPage++;

			AddEntry(page, len, data);
		}
		else
		{
			std::map<id_type, Entry*>::iterator it = pageIndex.find(page);
			if (it == pageIndex.end())
			{
				AddEntry(page, len, data);
				it = pageIndex.find(page);
			}
			else
			{
				if (len > it->second->length)
				{
					delete[] it->second->data;
					it->second->data = new byte[len];
				}
				it->second->length = len;
				memcpy(it->second->data, data, len);
				it->second->useIndex = currentUseIndex;
			}
		}

		currentUseIndex++;
	}

	virtual void deleteByteArray(const id_type page)
	{
		map<id_type, Entry*>::iterator it = pageIndex.find(page);
		if (it == pageIndex.end()) throw InvalidPageException(page);

		delete it->second;
		pageIndex.erase(it);

		wasChanged = true;
		writeBatch.Delete(IdToSlice(page));
	}

private:

	void AddEntry(const id_type& page, const uint32_t len, const byte* const data)
	{
		Entry* e = new Entry();
		e->length = len;
		e->data = new byte[len];
		e->useIndex = currentUseIndex;
		memcpy(e->data, data, len);
		pageIndex.insert(std::pair<id_type, Entry*>(page, e));

		if (pageIndex.size() > 10000)
		{
			FlushLRUCache(pageIndex.size() / 2);
		}
	}

	void FlushLRUCache(const size_t numEntriesToFlush)
	{
		if(pageIndex.size() > 0)
		{
			vector<uint64_t> useIndices;
			useIndices.reserve(pageIndex.size());
			for (auto it = pageIndex.begin(); it != pageIndex.end(); ++it)
			{
				useIndices.push_back(it->second->useIndex);
			}
			sort(useIndices.begin(), useIndices.end());

			uint64_t limitUseIndex = useIndices[numEntriesToFlush - 1];

			for (auto it = pageIndex.begin(); it != pageIndex.end();)
			{
				if (it->second->useIndex <= limitUseIndex)
				{
					writeBatch.Put(IdToSlice(it->first), Slice((const char*)it->second->data, it->second->length));
					delete it->second;
					it = pageIndex.erase(it);
				}
				else
				{
					it++;
				}
			}
		}

		WriteOptions wo;
		db->Write(wo, &writeBatch);
		writeBatch.Clear();
	}

	class Entry
	{
	public:
		uint32_t length;
		byte* data;
		uint64_t useIndex;

		~Entry()
		{
			delete[] data;
		}
	};

	DB* db;
	Options options;
	bool wasChanged;
	WriteBatch writeBatch;

	id_type nextPage;
	uint64_t currentUseIndex;
	std::map<id_type, Entry*> pageIndex;
};

void ReadWay(O5mreader* reader, DB* db, ISpatialIndex* tree, const uint64_t& wayId)
{
	Way way;
	way.id = wayId;

	BBox bb;
	bb.minX = numeric_limits<int32_t>::max();
	bb.minY = numeric_limits<int32_t>::max();
	bb.maxX = numeric_limits<int32_t>::min();
	bb.maxY = numeric_limits<int32_t>::min();

	if (readNodeFromDB_T1 == high_resolution_clock::time_point::time_point())
	{
		readNodeFromDB_T1 = high_resolution_clock::now();
	}

	vector<uint64_t> nodeIds;
	nodeIds.reserve(1000);

	uint64_t nodeId;
	O5mreaderIterateRet ret;
	while ((ret = o5mreader_iterateNds(reader, &nodeId)) == O5MREADER_ITERATE_RET_NEXT) 
	{
		nodeIds.push_back(nodeId);
	}

	char *key, *val;
	while ((ret = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT)
	{
		Tag tag;
		tag.key = key;
		tag.value = val;
		way.tags.push_back(tag);
	}

	way.polygon.SetNumVertices((int)nodeIds.size());

	ReadOptions ro;
	auto iterator = db->NewIterator(ro);
	iterator->Seek(IdToSlice(nodeIds[0]));

	int nodeIndex = 0;
	while (iterator->Valid())
	{
		uint64_t nodeId = nodeIds[nodeIndex];
		uint64_t iteratorId = SliceToId(iterator->key());

		int64_t delta = nodeId - iteratorId;
		if (delta > 0)
		{
			if (delta > 5)
			{
				iterator->Seek(IdToSlice(nodeId));
				continue;
			}
			else
			{
				iterator->Next();
				continue;
			}
		}
		if (delta < 0)
		{
			if (delta < 5)
			{
				iterator->Seek(IdToSlice(nodeId));
				continue;
			}
			else
			{
				iterator->Prev();
				continue;
			}
		}

		Slice value = iterator->value();
		NodeValue& node = *((NodeValue*)value.data());

		bb.minX = Min(bb.minX, node.lon);
		bb.minY = Min(bb.minY, node.lat);
		bb.maxX = Max(bb.maxX, node.lon);
		bb.maxY = Max(bb.maxY, node.lat);

		TVector2D<O5MCoord> nodeLocation(node.lon, node.lat);
		way.polygon.SetVertex(nodeIndex, nodeLocation);

		numDBReadNodes++;
		nodeIndex++;
		if (nodeIndex >= nodeIds.size()) break;

		iterator->Next();
	}

	bool outputNodeTags = false;
	if (nodeIndex < nodeIds.size())
	{
		cout << "Broken Way! Nodes are missing! wayId: " << wayId << endl;
		for (size_t i = 0; i < way.tags.size(); i++)
		{
			cout << way.tags[i].key << " --- " << way.tags[i].value << endl;
		}
	}

	delete iterator;

	way.bbox = bb;

	MGArchive archive;
	archive << way;

	uint64_t waySize = 0;
	char* serializedWay = archive.ToByteStream(waySize);

	O5MCoord sizeX = bb.maxX - bb.minX;
	O5MCoord sizeY = bb.maxY - bb.minY;
	O5MCoord sizeMin = min(sizeX, sizeY);
	O5MCoord sizeMax = min(sizeX, sizeY);

	double min[3]{ bb.minX * 0.0000001, bb.minY * 0.0000001, sizeMin * 0.0000001 };
	double max[3]{ bb.maxX * 0.0000001, bb.maxY * 0.0000001, sizeMax * 0.0000001 };
	Region siBB(min, max, 3);
	tree->insertData((uint32_t)waySize, (const byte*)serializedWay, siBB, wayId);

	delete[] serializedWay;
}

class GetAllWays : public IVisitor
{
public:
	vector<uint64_t> ways;

	virtual void visitNode(const INode& in) 
	{
	}
	virtual void visitData(const IData& in) 
	{
		uint64_t id = in.getIdentifier();
		ways.push_back(id);

		const bool doDataDeserialization = false;
		if(doDataDeserialization)
		{
			uint32_t dataLen;
			byte* data;
			in.getData(dataLen, &data);

			MGArchive archive((const char*)data, dataLen);
			Way way;
			archive << way;

			for (size_t i = 0; i < way.tags.size(); i++)
			{
				//if (way.tags[i].key == "area")
				{
					ways.push_back(id);
					break;
				}
			}

			delete data;
		}
	}
	virtual void visitData(std::vector<const IData*>& v) 
	{
		cout << "Error: visitData (multi data) not implemented" << endl;
	}
};

void TestSpatialIndexSpeed(string& wayDBFilePath)
{
	IStorageManager* diskfile = new CachedDiskStorageManager(wayDBFilePath);
	ISpatialIndex* tree = loadRTree(*diskfile, 1);

	high_resolution_clock::time_point t1 = high_resolution_clock::now();

	GetAllWays getAllWays;

	//double min[3]{ 4.0, 52.0, 0.002 };
	//double max[3]{ 5.0, 53.0, 500.0 };
	double min[3]{ -180.0, -90.0, 0.0 };
	double max[3]{ 180.0, 90.0, 500.0 };
	Region queryAABB(min, max, 3);
	tree->intersectsWithQuery(queryAABB, getAllWays);

	cout << "Num Ways returned: " << getAllWays.ways.size() << "                               " << endl;
	high_resolution_clock::time_point t2 = high_resolution_clock::now();
	duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

	cout << "Num Query took seconds: " << time_span.count() << "                               " << endl;
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

	string baseFile = "netherlands.osm.o5m";
	//string baseFile = "antarctica-2016-01-06.osm.o5m";

	string root = "../test/files/";
	string nodeDBFilePath = root + baseFile + ".nd-idx";
	string wayDBFilePath = root + baseFile + ".way";

	//TestSpatialIndexSpeed(wayDBFilePath);
	//return 0;

	SpatialIndex::id_type indexId = 0;
	IStorageManager* diskfile = new CachedDiskStorageManager(wayDBFilePath);
	ISpatialIndex* tree = createNewRTree(*diskfile, 0.7, 100, 100, 3, RV_RSTAR, indexId);

	DB* db = NULL;	
	bool writeDB = !experimental::filesystem::exists(nodeDBFilePath);
	Options options;
	options.comparator = new OSMIdComparator();
	options.create_if_missing = true;
	options.write_buffer_size = 100 << 20;
	options.filter_policy = NewBloomFilterPolicy(32);
	DB::Open(options, nodeDBFilePath, &db);

	string srcFilePath = "../test/files/" + baseFile;
	FILE* f = fopen(srcFilePath.c_str(), "rb");
	
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

				if(writeDB)
				{
					NodeValue nv;
					nv.lon = ds.lon;
					nv.lat = ds.lat;
					nv.fileOffset = reader->f->fOffset;
					nv.readerOffset = reader->offset;

					wb.Put(IdToSlice(ds.id), Slice((const char*)&nv, sizeof(NodeValue)));
				}
				break;
			}
			case O5MREADER_DS_WAY:

				if (writeDB) // write last few node entries before reading ways
				{
					writeDB = false;
					db->Write(wo, &wb);
					wb.Clear();

					db->CompactRange(NULL, NULL);
				}

				ReadWay(reader, db, tree, ds.id);
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

		int numNodesReadIntermediate = numDBReadNodes - numDBReadNodesPreviously;
		if (numNodesReadIntermediate > 100000)
		{
			high_resolution_clock::time_point readNodeFromDB_T2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(readNodeFromDB_T2 - readNodeFromDB_T1);
			
			cout << "Num Nodes read per second: " << (int)(numNodesReadIntermediate / time_span.count()) << "                         \r";

			numDBReadNodesPreviously = numDBReadNodes;
			readNodeFromDB_T1 = readNodeFromDB_T2;
		}

		if ((numDataSetsRead & 0x7FFFF) == 0)
		{
			if(writeDB)
			{
				db->Write(wo, &wb);
				wb.Clear();
			}

			uint64_t numDataSetsReadIntermediate = numDataSetsRead - numDataSetsReadPreviously;
			high_resolution_clock::time_point t2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

			cout << "Num Datasets read per second: " << (int)(numDataSetsReadIntermediate / time_span.count()) << "                               \r";

			numDataSetsReadPreviously = numDataSetsRead;
			t1 = t2;
		}
	}

	delete options.comparator;
	delete db;
	delete options.filter_policy;

	delete tree;
	delete diskfile;

	o5mreader_close(reader);
	fclose(f);

	cout << "Num Nodes read: " << numDBReadNodes << "                               " << endl;
	cout << "Num Datasets read: " << numDataSetsRead << "                               " << endl;

	return 0;
 }
