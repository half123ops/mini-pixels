//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <algorithm>
#include <ctime>       
#include <iomanip>    
#include <sstream>     

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(1) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
	ColumnVector::ensureSize(size,preserveData);
    if (length < size) {
        int *oldDates = dates;
        posix_memalign(reinterpret_cast<void**>(&dates), 32, size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);
        }
		delete[] oldDates;
        memoryUsage += sizeof(int32_t) * (size - length);
        resize(size);
    }
}

void DateColumnVector::add(std::string &value) {
	std::transform(value.begin(), value.end(), value.begin(), ::tolower);
	std::cout << "Adding string value: " << value << std::endl;
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
    	std::tm tm = {};
    	std::istringstream ss(value);
    	ss >> std::get_time(&tm, "%Y-%m-%d");
    	if (ss.fail()) {
        	throw std::invalid_argument("Invalid date format: " + value);
    	}
    
    	std::time_t time = std::mktime(&tm);
    	if (time == -1) {
        	throw std::invalid_argument("Failed to convert date to time_t: " + value);
    	}

    	// 计算天数，1970年1月1日到给定日期的天数
    	std::tm epoch = {};
    	epoch.tm_year = 70; 
    	epoch.tm_mon = 0;   
    	epoch.tm_mday = 1;  
    	std::time_t epochTime = std::mktime(&epoch);
    	int days = std::difftime(time, epochTime) / (60 * 60 * 24); // 转换为天数

    	add(days); // 使用 days 来调用 add(int value)
	}
}

void DateColumnVector::add(bool value) {
	std::cout << "Adding bool value: " << value << std::endl;
    add(value ? 1 : 0);
}

void DateColumnVector::add(int64_t value) {
	std::cout << "Adding int64_t value: " << value << std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = static_cast<int>(value); 
	isNull[index] = false;
}

void DateColumnVector::add(int value) {
	std::cout << "Adding int value: " << value << std::endl;
	std::cout << "writeIndex:" << writeIndex << " " << "length:" << length << std::endl;
    if (writeIndex >= length) {
		std::cout << "ing..." << std::endl;
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
	isNull[index] = false;
}