//
// Created by liyu on 12/23/23.
//

#include <sstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <ctime>
#include <stdexcept>
#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if (1) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}

void TimestampColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
}

TimestampColumnVector::~TimestampColumnVector() {
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    if (this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        if (encoding) {
            long *oldTimes = times;
            posix_memalign(reinterpret_cast<void **>(&times), 64, size * sizeof(long));
            if (preserveData) {
                std::copy(oldTimes, oldTimes + length, times);
            }
            delete[] oldTimes;
        } else {
            long *oldTimes = times;
            times = new long[size];
            if (preserveData) {
                std::copy(oldTimes, oldTimes + length, times);
            }
            delete[] oldTimes;
        }
        memoryUsage += (size - length) * sizeof(long);
        resize(size);
    }
}

void TimestampColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    std::cout << "Adding string value: " << value << std::endl;

    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
        std::tm tm = {};
        std::istringstream ss(value);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

        if (ss.fail()) {
            throw std::invalid_argument("Invalid timestamp format: " + value);
        }

        std::time_t time = std::mktime(&tm);
        if (time == -1) {
            throw std::invalid_argument("Failed to convert timestamp to time_t: " + value);
        }

        long timestamp = static_cast<long>(time) + 8*3600;

        if (precision == 0) precision = 6;

        std::string::size_type pos = value.find('.');  
        if (pos != std::string::npos) {
            std::string fractional = value.substr(pos + 1);  
            if (precision == 3) {
                int ms = std::stoi(fractional.substr(0, 3)); 
                timestamp *= 1000;  
                timestamp += ms;
            } else if (precision == 6) {
                int us = std::stoi(fractional.substr(0, 6));  
                timestamp *= 1000000;  
                timestamp += us;
            }
        }
        else{
            if (precision == 3) {
                timestamp *= 1000;  
            } else if (precision == 6) {
                timestamp *= 1000000;  
            }
        }
        add(timestamp);
    }
}

void TimestampColumnVector::add(long value) {
    std::cout << "Adding long value: " << value << std::endl;

    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    int index = writeIndex++;
    times[index] = value;
    isNull[index] = false;
}

void TimestampColumnVector::add(int value) {
    std::cout << "Adding int value: " << value << std::endl;
    add(static_cast<long>(value));  
}

void TimestampColumnVector::add(bool value) {
    std::cout << "Adding bool value: " << value << std::endl;
    add(value ? 1 : 0); 
}
