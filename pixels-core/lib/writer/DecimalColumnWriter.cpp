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
#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption) {}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    std::cout << "In DecimalColumnWriter" << std::endl;

    // 转换为 DecimalColumnVector 类型
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type");
    }

    long* values = columnVector->vector;  
    std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
    EncodingUtils encodingUtils;

    for (int i = 0; i < length; i++) {
        isNull[curPixelIsNullIndex++] = vector->isNull[i]; 
        curPixelEleIndex++;

        curVecPartitionBuffer = std::make_shared<ByteBuffer>(sizeof(long));  

        if (vector->isNull[i]) {
            hasNull = true;
            if (nullsPadding) {
                // 如果是空值，且启用了填充，则填充 0
                encodingUtils.writeLongLE(curVecPartitionBuffer, 0L);
            }
        } else {
            // 如果不是空值，按字节序写入数据
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
                encodingUtils.writeLongLE(curVecPartitionBuffer, values[i]);
            } else {
                encodingUtils.writeLongBE(curVecPartitionBuffer, values[i]);
            }
        }

        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());

        if (curPixelEleIndex >= pixelStride) {
            ColumnWriter::newPixel();
        }
    }

    return outputStream->getWritePos(); 
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    // 决定是否启用空值填充
    return writerOption->isNullsPadding();
}
