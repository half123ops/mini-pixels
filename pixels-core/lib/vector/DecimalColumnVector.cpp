//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <iostream>
#include <string>
#include <cmath>
#include <algorithm>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, vector);
        }
        delete[] oldVector;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);  
    }
}


void DecimalColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    std::cout << "add string value:" << value << std::endl;
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
        int pos = value.find('.');
        if (pos == std::string::npos) {
            // 没有小数点的情况，处理为整数
            long lv = std::stol(value);  
            int64_t v = lv * std::pow(10, scale); 
            add(v); 
        } else {
            // 有小数点的情况，去掉小数点后将其处理为整数
            int64_t lv = std::stol(value.substr(0, pos) + value.substr(pos + 1));
            int64_t v = lv * std::pow(10, scale - (value.length() - pos - 1));  // 根据刻度和小数点后位数调整
            add(v);
        }
    }
}

void DecimalColumnVector::add(bool value) {
    std::cout << "add bool value:" << value << std::endl;
    add(value ? 1 : 0);
}

void DecimalColumnVector::add(int64_t value) {
    std::cout << "add int64_t value:" << value << std::endl;
    std::cout << "writeIndex:" << writeIndex << std::endl;
    std::cout << "length:" << length << std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);  
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;  
}

void DecimalColumnVector::add(int value) {
    std::cout << "add int value:" << value << std::endl;
    add(static_cast<int64_t>(value));  
}