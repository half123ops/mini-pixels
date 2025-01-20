/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/TimestampColumnWriter.h"
#include "utils/EncodingUtils.h"
#include "utils/BitUtils.h"
#include <iostream>
#include <memory>
#include <vector>

TimestampColumnWriter::TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
    ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    // 确保编码级别判断正确
    runlengthEncoding = writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2); 
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>(); // 初始化游程编码器
    }
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length)
{
    std::cout << "In TimestampColumnWriter" << std::endl;

    auto columnVector = std::static_pointer_cast<LongColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type"); // 类型不匹配时抛出异常
    }

    long* values;
    if (columnVector->isLongVector()) {
        values = columnVector->longVector;
    } else {
        values = columnVector->intVector; // 如果是整型向量，则转换为 long 类型
    }

    int curPartLength;         // 当前分区的大小
    int curPartOffset = 0;     // 当前分区的起始偏移
    int nextPartLength = length; // 下一分区的大小

    // 对分区进行预计算，避免在主循环中进行分支预测
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTimestamp(columnVector, values, curPartLength, curPartOffset);
        newPixel(); // 写入当前像素
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartTimestamp(columnVector, values, curPartLength, curPartOffset); // 最后一部分

    return outputStream->getWritePos(); // 返回当前写入的位置
}

void TimestampColumnWriter::close()
{
    if (runlengthEncoding && encoder) {
        encoder->clear(); // 清理游程编码器
    }
    ColumnWriter::close(); // 调用基类的 close 方法
}

void TimestampColumnWriter::writeCurPartTimestamp(std::shared_ptr<ColumnVector> columnVector, long* values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) // 检查当前值是否为空
        {
            hasNull = true;
            if (nullsPadding)
            {
                // 如果启用了 null 填充，则填充 0
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset]; // 否则直接写入值
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

bool TimestampColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    // 如果编码级别大于等于 EL2，则不进行 null 填充
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2)) {
        return false;
    }
    return writerOption->isNullsPadding(); // 返回是否进行 null 填充
}

void TimestampColumnWriter::newPixel()
{
    // 确保 runlengthEncoding 为 true 才使用游程编码
    if (runlengthEncoding) {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen); // 使用游程编码
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        
        curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(long));
        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }
        else
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel(); // 调用基类的 newPixel 方法
}

pixels::proto::ColumnEncoding TimestampColumnWriter::getColumnChunkEncoding()
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH); // 设置编码类型为游程编码
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE); // 无编码
    }
    return columnEncoding;
}
