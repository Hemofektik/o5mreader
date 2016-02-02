#ifndef __MGARCHIVE_H__
#define __MGARCHIVE_H__

#include <sstream>
#include <assert.h>

using namespace std;

class MGArchive
{
protected:
	stringstream m_s;

private:
	bool m_isLoading;
	bool m_isSaving;

public:
	/**************************** CTOR ****************************/

	MGArchive()
		: m_s("", ios_base::in | ios_base::out | ios_base::binary)
		, m_isLoading(false)
		, m_isSaving(true)
	{
	}

	MGArchive(const char* data, unsigned int size)
		: m_s("", ios_base::in | ios_base::out | ios_base::binary)
		, m_isLoading(true)
		, m_isSaving(false)
	{
		m_s.write(data, size);
		m_s.seekg(0);
	}

	MGArchive(MGArchive& ar)
		: m_s("", ios_base::in | ios_base::out | ios_base::binary)
		, m_isLoading(true)
		, m_isSaving(false)
	{
		// TODO: Find non-intrusive method
		ar.m_s.seekg(0, ios_base::end);
		const int size = (int)ar.m_s.tellg();

		char *tempBuffer = new char[size];

		// Read original data
		ar.m_s.seekg(0);
		ar.m_s.read(tempBuffer, size);

		// Write to stream
		m_s.seekp(0);
		m_s.write(tempBuffer, size);

		// Delete temp
		delete tempBuffer;
	}

	/**************************** GETTER ****************************/

	bool IsLoading() const { return m_isLoading; };
	bool IsSaving() const { return m_isSaving; };

	char* ToByteStream(uint64_t& size)
	{
		// Size determination
		m_s.seekg(0, ios_base::end);
		size = (uint64_t)m_s.tellg();

		char* result = new char[size];
		m_s.seekg(0);

		m_s.read(result, size);

		return result;
	}

	/**************************** WRITING ****************************/
	template <typename T>
	void Serialize(const T& value)
	{
		assert(m_isSaving);

		m_s.write((char*)&value, sizeof(T));
	}

	void Serialize(const char* data, const uint64_t size)
	{
		assert(m_isSaving);
		m_s.write(data, size);
	}

	/**************************** LOADING ****************************/
	template <typename T>
	T Serialize()
	{
		assert(m_isLoading);

		T result;
		m_s.read((char*)&result, sizeof(T));

		return result;
	}

	void Serialize(char* data, const uint64_t& size)
	{
		assert(m_isLoading);
		m_s.read(data, size);
	}
};

template<typename T>
MGArchive& operator<<(MGArchive& archive, T& value)
{
	if (archive.IsLoading())
	{
		value = archive.Serialize<T>();
	}
	else
	{
		archive.Serialize(value);
	}
	return archive;
}

template<>
MGArchive& operator<<(MGArchive& archive, std::string& value)
{
	if (archive.IsLoading())
	{
		uint64_t length = archive.Serialize<uint64_t>();
		value.resize(length);
		archive.Serialize(&value.front(), length);
	}
	else
	{
		uint64_t length = value.length();
		archive.Serialize(length);
		archive.Serialize(value.c_str(), length);
	}
	return archive;
}

#endif // __MGARCHIVE_H__