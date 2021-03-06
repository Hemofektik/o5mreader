
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
#include "vector_tile.pb.h"

using namespace std;
using namespace std::chrono;
using namespace leveldb;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
using namespace ZFXMath;

const int SEG_MOVETO = 1;
const int SEG_LINETO = 2;
const int SEG_CLOSE = 7;

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
static double avgNumNodesPerWay = 0.0;

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

// CachedDiskStorageManager vs. DiskStorageManager
// advantages:
//  - reduces disk accesses to a minimum
//  - reduces disk usage by about 40%
//  - keeping track of index is not memory bound (index is only kept partially in memory)
// disadvantages
//  - leveldb dependency
class CachedDiskStorageManager : public SpatialIndex::IStorageManager
{
	const int MaxNumPagesHeldInMemory = 10000;

public:
	CachedDiskStorageManager(const string& filePath)
		: db(NULL)
		, isDirty(false)
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
		FlushLRUCache(pageIndex.size());

		if (isDirty)
		{
			db->CompactRange(NULL, NULL);
			isDirty = false;
		}
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
				AddEntry(page, len, (const byte*)result.data(), false);
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
		isDirty = true;

		if (page == StorageManager::NewPage)
		{
			page = nextPage;
			nextPage++;

			AddEntry(page, len, data, true);
		}
		else
		{
			std::map<id_type, Entry*>::iterator it = pageIndex.find(page);
			if (it == pageIndex.end())
			{
				AddEntry(page, len, data, true);
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
				it->second->isDirty = true;
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

		isDirty = true;
		writeBatch.Delete(IdToSlice(page));
	}

private:

	void AddEntry(const id_type& page, const uint32_t len, const byte* const data, bool isDirty)
	{
		Entry* e = new Entry();
		e->length = len;
		e->data = new byte[len];
		e->useIndex = currentUseIndex;
		e->isDirty = isDirty;
		memcpy(e->data, data, len);
		pageIndex.insert(std::pair<id_type, Entry*>(page, e));

		if (pageIndex.size() > MaxNumPagesHeldInMemory)
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
					if (it->second->isDirty)
					{
						writeBatch.Put(IdToSlice(it->first), Slice((const char*)it->second->data, it->second->length));
					}
					delete it->second;
					it = pageIndex.erase(it);
				}
				else
				{
					it++;
				}
			}
		}

		if(isDirty)
		{
			WriteOptions wo;
			db->Write(wo, &writeBatch);
			writeBatch.Clear();
		}
	}

	class Entry
	{
	public:
		uint32_t length;
		byte* data;
		uint64_t useIndex;
		bool isDirty;

		~Entry()
		{
			delete[] data;
		}
	};

	DB* db;
	Options options;
	bool isDirty;
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
	O5MCoord sizeMax = max(sizeX, sizeY);

	double min[3]{ bb.minX * 0.0000001, bb.minY * 0.0000001, sizeMin * 0.0000001 };
	double max[3]{ bb.maxX * 0.0000001, bb.maxY * 0.0000001, sizeMax * 0.0000001 };
	Region siBB(min, max, 3);
	tree->insertData((uint32_t)waySize, (const byte*)serializedWay, siBB, wayId);

	avgNumNodesPerWay = avgNumNodesPerWay * 0.9 + nodeIds.size() * 0.1;

	delete[] serializedWay;
}

class GetAllWaysWithKey : public IVisitor
{
public:
	vector<uint64_t> ways;
	string key;

	GetAllWaysWithKey(const string& key)
		: key(key)
	{
	}

	virtual void visitNode(const INode& in) 
	{
	}
	virtual void visitData(const IData& in) 
	{
		uint64_t id = in.getIdentifier();

		const bool doDataDeserialization = true;
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
				if (way.tags[i].key == key)
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

	GetAllWaysWithKey getAllWays("highway");

	double min[3]{ 4.0, 52.0, 0.004 };
	double max[3]{ 5.0, 53.0, 500.0 };
	//double min[3]{ -180.0, -90.0, 0.0 };
	//double max[3]{ 180.0, 90.0, 500.0 };
	Region queryAABB(min, max, 3);
	tree->intersectsWithQuery(queryAABB, getAllWays);

	cout << "Num Ways returned: " << getAllWays.ways.size() << "                               " << endl;
	high_resolution_clock::time_point t2 = high_resolution_clock::now();
	duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

	cout << "Num Query took seconds: " << time_span.count() << "                               " << endl;
}

class Polygon
{
public:
	uint64_t id;
	TPolygon2D<double>* polygons;
	int numPolygons;

	Polygon(const uint64_t id, const double scale, const TPolygon2D<int32_t>& exterior, 
			const TPolygon2D<int32_t>* interior = NULL, 
			const uint32_t numInteriors = 0)
		: id(id)
		, polygons(new TPolygon2D<double>[1 + numInteriors])
		, numPolygons(1 + numInteriors)
	{
		// copy outer perimeter
		{
			polygons[0] = exterior.Clone<double>(scale);
			polygons[0].CloseRing();

			double area = polygons[0].ComputeArea();
			assert(area <= 0.0);
		}

		// create inner holes
		{
			for (uint32_t p = 0; p < numInteriors; p++)
			{
				polygons[p + 1] = interior[p].Clone<double>(scale);
				polygons[p + 1].CloseRing();

				double area = polygons[p + 1].ComputeArea();
				assert(area >= 0.0);
			}
		}
	}

	Polygon(Polygon&& polygon)
		: id(polygon.id)
		, polygons(polygon.polygons)
		, numPolygons(polygon.numPolygons)
	{
		polygon.polygons = NULL;
	} // copy constructor is implicitly forbidden due to user-defined move constructor (use Clone() instead)

	Polygon& operator= (Polygon&& polygon)
	{
		this->id = polygon.id;
		this->polygons = polygon.polygons;
		this->numPolygons = polygon.numPolygons;
		polygon.polygons = NULL;
		return *this;
	} // copy assignment is implicitly forbidden due to user-defined move assignment (use Clone() instead)

	~Polygon()
	{
		delete[] polygons;
	}

	double ComputeSignedSquareDistance(double x, double y) const
	{
		bool pointIsRightOfEdge = false;
		TVector2D<double> point(x, y);
		double sqrDistance = polygons[0].ComputeSqrDistance(point, pointIsRightOfEdge);

		for (int p = 1; p < numPolygons; p++)
		{
			bool pointIsRightOfInnerEdge = false;
			double innerSqrDistance = polygons[p].ComputeSqrDistance(point, pointIsRightOfInnerEdge);
			if (innerSqrDistance < sqrDistance)
			{
				sqrDistance = innerSqrDistance;
				pointIsRightOfEdge = pointIsRightOfInnerEdge;
			}
		}

		return pointIsRightOfEdge ? -sqrDistance : sqrDistance;
	}
};

int32_t ZigZagUint32ToInt32(uint32_t zigzag)
{
	return (int32_t)((zigzag & 1) ? -(int64_t)(zigzag >> 1) - 1 : (int64_t)(zigzag >> 1));
}

void OutputTile(bool verbose, vector_tile::Tile& tile)
{
	if (!verbose) {
		std::cout << "layers: " << static_cast<std::size_t>(tile.layers_size()) << "\n";
		for (std::size_t i = 0; i<static_cast<std::size_t>(tile.layers_size()); ++i)
		{
			vector_tile::Tile_Layer const& layer = tile.layers(i);
			const double tileScale = 1.0 / layer.extent();

			std::cout << layer.name() << ":\n";
			std::cout << "  version: " << layer.version() << "\n";
			std::cout << "  extent: " << layer.extent() << "\n";
			std::cout << "  features: " << static_cast<std::size_t>(layer.features_size()) << "\n";
			std::cout << "  keys: " << static_cast<std::size_t>(layer.keys_size()) << "\n";
			std::cout << "  values: " << static_cast<std::size_t>(layer.values_size()) << "\n";
			unsigned total_repeated = 0;
			unsigned num_commands = 0;
			unsigned num_move_to = 0;
			unsigned num_line_to = 0;
			unsigned num_close = 0;
			unsigned num_empty = 0;
			unsigned degenerate = 0;

			vector<Polygon> polygons;
			vector<TPolygon2D<double>> lineStrings;
			uint64_t lastFeatureId = 0;
			for (std::size_t j = 0; j<static_cast<std::size_t>(layer.features_size()); ++j)
			{
				int32_t cursorX = 0;
				int32_t cursorY = 0;
				vector_tile::Tile_Feature const & f = layer.features(j);
				total_repeated += f.geometry_size();
				int cmd = -1;
				const int cmd_bits = 3;
				unsigned length = 0;
				unsigned g_length = 0;
				vector<TPolygon2D<int32_t>> polys;
				TPolygon2D<int32_t> poly;
				for (int k = 0; k < f.geometry_size();)
				{
					if (!length) {
						unsigned cmd_length = f.geometry(k++);
						cmd = cmd_length & ((1 << cmd_bits) - 1);
						length = cmd_length >> cmd_bits;
						if (length <= 0) num_empty++;
						num_commands++;
					}
					if (length > 0) {
						length--;
						if (cmd == SEG_MOVETO || cmd == SEG_LINETO)
						{
							uint32_t xZigZag = f.geometry(k++);
							uint32_t yZigZag = f.geometry(k++);
							int32_t xRel = ZigZagUint32ToInt32(xZigZag);
							int32_t yRel = ZigZagUint32ToInt32(yZigZag);
							cursorX += xRel;
							cursorY += yRel;

							g_length++;
							if (cmd == SEG_MOVETO)
							{
								if (poly.GetNumVertices() > 0)
								{
									polys.push_back(move(poly));
									poly = TPolygon2D<int32_t>();
									poly.ReserveNumVertices(10);
								}

								num_move_to++;
							}
							else if (cmd == SEG_LINETO)
							{
								num_line_to++;
							}

							poly.AddVertex(TVector2D<int32_t>(cursorX, cursorY));
						}
						else if (cmd == (SEG_CLOSE & ((1 << cmd_bits) - 1)))
						{
							if (g_length <= 2) degenerate++;
							g_length = 0;
							num_close++;

							polys.push_back(move(poly));
							poly = TPolygon2D<int32_t>();
							poly.ReserveNumVertices(10);
						}
						else
						{
							std::stringstream s;
							s << "Unknown command type: " << cmd;
							throw std::runtime_error(s.str());
						}
					}
				}

				if (f.type() == vector_tile::Tile_GeomType_POLYGON)
				{
					size_t polyStartIndex = 0;
					for (size_t p = 0; p < polys.size(); p++)
					{
						int64_t polyArea = polys[p].ComputeArea<int64_t>();
						if (p > polyStartIndex && polyArea < 0) // test for multipolygons including interior polys
						{
							auto& poly = polys[polyStartIndex];
							const auto* nextPoly = &polys[polyStartIndex + 1];
							Polygon polygon(f.id(), tileScale, poly,
								(polyStartIndex + 1 < polys.size() ) ? nextPoly : NULL,
								(uint32_t)(p - polyStartIndex - 1));
							polygons.push_back(move(polygon));

							polyStartIndex = p;
						}
					}
					if (polyStartIndex < polys.size())
					{
						Polygon polygon(f.id(), tileScale, polys[polyStartIndex],
							(polyStartIndex + 1 < polys.size()) ? &polys[polyStartIndex + 1] : NULL,
							(uint32_t)(polys.size() - 1 - polyStartIndex));
						polygons.push_back(move(polygon));
					}
				}
				else if (f.type() == vector_tile::Tile_GeomType_LINESTRING)
				{
					polys.push_back(move(poly));

					for (const auto& p : polys)
					{
						lineStrings.push_back(move(p.Clone<double>(tileScale)));
					}
				}
			}
			std::cout << "  geometry summary:\n";
			std::cout << "    total: " << total_repeated << "\n";
			std::cout << "    commands: " << num_commands << "\n";
			std::cout << "    move_to: " << num_move_to << "\n";
			std::cout << "    line_to: " << num_line_to << "\n";
			std::cout << "    close: " << num_close << "\n";
			std::cout << "    degenerate polygons: " << degenerate << "\n";
			std::cout << "    empty geoms: " << num_empty << "\n";
			std::cout << "    NUM POLYGONS: " << polygons.size() << "\n";
			std::cout << "    NUM LINE STRINGS: " << lineStrings.size() << "\n";
		}
	}
	else {
		for (std::size_t j = 0; j < static_cast<std::size_t>(tile.layers_size()); ++j)
		{
			vector_tile::Tile_Layer const& layer = tile.layers(j);
			std::cout << "layer: " << layer.name() << "\n";
			std::cout << "  version: " << layer.version() << "\n";
			std::cout << "  extent: " << layer.extent() << "\n";
			std::cout << "  keys: ";
			for (std::size_t k = 0; k < static_cast<std::size_t>(layer.keys_size()); ++k)
			{
				std::string const& key = layer.keys(k);
				std::cout << key;
				if (k < static_cast<std::size_t>(layer.keys_size()) - 1) {
					std::cout << ",";
				}
			}
			std::cout << "\n";
			std::cout << "  values: ";
			for (std::size_t l = 0; l < static_cast<std::size_t>(layer.values_size()); ++l)
			{
				vector_tile::Tile_Value const & value = layer.values(l);
				if (value.has_string_value()) {
					std::cout << value.string_value();
				}
				else if (value.has_int_value()) {
					std::cout << value.int_value();
				}
				else if (value.has_double_value()) {
					std::cout << value.double_value();
				}
				else if (value.has_float_value()) {
					std::cout << value.float_value();
				}
				else if (value.has_bool_value()) {
					std::cout << value.bool_value();
				}
				else if (value.has_sint_value()) {
					std::cout << value.sint_value();
				}
				else if (value.has_uint_value()) {
					std::cout << value.uint_value();
				}
				else {
					std::cout << "null";
				}
				if (l < static_cast<std::size_t>(layer.values_size()) - 1) {
					std::cout << ",";
				}
			}
			std::cout << "\n";
			for (std::size_t l = 0; l < static_cast<std::size_t>(layer.features_size()); ++l)
			{
				vector_tile::Tile_Feature const & feat = layer.features(l);
				std::cout << "  feature: " << feat.id() << "\n";
				std::cout << "    type: ";
				unsigned feat_type = feat.type();
				if (feat_type == 0) {
					std::cout << "Unknown";
				}
				else if (feat_type == vector_tile::Tile_GeomType_POINT) {
					std::cout << "Point";
				}
				else if (feat_type == vector_tile::Tile_GeomType_LINESTRING) {
					std::cout << "LineString";
				}
				else if (feat_type == vector_tile::Tile_GeomType_POLYGON) {
					std::cout << "Polygon";
				}
				std::cout << "\n";
				std::cout << "    tags: ";
				for (std::size_t m = 0; m < static_cast<std::size_t>(feat.tags_size()); ++m)
				{
					uint32_t tag = feat.tags(j);
					std::cout << tag;
					if (m < static_cast<std::size_t>(feat.tags_size()) - 1) {
						std::cout << ",";
					}
				}
				std::cout << "\n";
				std::cout << "    geometries: ";
				for (std::size_t m = 0; m < static_cast<std::size_t>(feat.geometry_size()); ++m)
				{
					uint32_t geom = feat.geometry(m);
					std::cout << geom;
					if (m < static_cast<std::size_t>(feat.geometry_size()) - 1) {
						std::cout << ",";
					}
				}
				std::cout << "\n";
			}
			std::cout << "\n";
		}
	}
}

void ReadVectorTileFromPBF()
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	vector_tile::Tile tile;

	{
		fstream input("../test/files/0-0-0.pbf", ios::in | ios::binary);
		input.seekg(0, input.end);
		uint64_t size = input.tellg();
		input.seekg(0, input.beg);
		char* buffer = new char[size];
		input.read(buffer, size);

		if (!tile.ParseFromArray(buffer, (int)size)) 
		{
			cerr << "Failed to parse tile." << endl;
			return;
		}

		OutputTile(false, tile);

		delete buffer;
	}


	google::protobuf::ShutdownProtobufLibrary();
}

#pragma optimize( "", on )
int main() 
{
	ReadVectorTileFromPBF();
	return 0;

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

	const bool PerformSpeedTest = false;
	if(PerformSpeedTest)
	{
		TestSpatialIndexSpeed(wayDBFilePath);
		return 0;
	}

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
		bool readingNodes = true;
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
				readingNodes = false;
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
				readingNodes = false;
				break;
		}

		int numNodesReadIntermediate = numDBReadNodes - numDBReadNodesPreviously;
		/*if (numNodesReadIntermediate > 100000)
		{
			high_resolution_clock::time_point readNodeFromDB_T2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(readNodeFromDB_T2 - readNodeFromDB_T1);
			
			int nodesPerSecond = (int)(numNodesReadIntermediate / time_span.count());
			cout << "Nodes/s: " << nodesPerSecond << " ("<< (int)avgNumNodesPerWay <<" nodes per way ("<< (int)(nodesPerSecond / avgNumNodesPerWay) << " ways/s)\r";

			numDBReadNodesPreviously = numDBReadNodes;
			readNodeFromDB_T1 = readNodeFromDB_T2;
		}*/

		int outputInterval = readingNodes ? 0x7FFFF : 0x7FFF;
		if ((numDataSetsRead & outputInterval) == 0)
		{
			if(writeDB)
			{
				db->Write(wo, &wb);
				wb.Clear();
			}

			uint64_t numDataSetsReadIntermediate = numDataSetsRead - numDataSetsReadPreviously;
			high_resolution_clock::time_point t2 = high_resolution_clock::now();
			duration<double> time_span = duration_cast<duration<double>>(t2 - t1);

			string unit = readingNodes ? "nodes/second: " : "ways/second: ";
			cout << unit << (int)(numDataSetsReadIntermediate / time_span.count()) << "                               \r";

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
