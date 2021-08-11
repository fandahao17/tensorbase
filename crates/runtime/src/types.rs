use std::{convert::TryFrom, fmt::Debug, intrinsics::copy_nonoverlapping, slice};

use arrow::{array, datatypes::DataType, record_batch::RecordBatch};
use base::{codec::encode_varint64, datetimes::TimeZoneId};
use bytes::{BufMut, BytesMut};
use client::prelude::{types::SqlType, ServerBlock, ValueRefEnum};
use meta::types::{AsBytes, BaseChunk, BqlType};

use crate::errs::{BaseRtError, BaseRtResult};

pub struct BaseColumn {
    pub name: Vec<u8>,
    pub data: BaseChunk,
}

#[derive(Default)]
pub struct BaseDataBlock {
    pub ncols: usize,
    pub nrows: usize,
    pub columns: Vec<BaseColumn>,
}

impl Debug for BaseDataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseDataBlock")
            .field("ncols", &self.ncols)
            .field("nrows", &self.nrows)
            .field("columns", &self.columns)
            .finish()
    }
}

// TODO FIXME: The best approach is to unify the server and client block definitions
// to avoid conversions
#[inline]
fn sqltype_to_bqltype(sqltype: SqlType) -> BqlType {
    match sqltype {
        SqlType::UInt8 => BqlType::UInt(8),
        SqlType::UInt16 => BqlType::UInt(16),
        SqlType::UInt32 => BqlType::UInt(32),
        SqlType::UInt64 => BqlType::UInt(64),
        SqlType::Int8 => BqlType::Int(8),
        SqlType::Int16 => BqlType::Int(16),
        SqlType::Int32 => BqlType::Int(32),
        SqlType::Int64 => BqlType::Int(64),
        SqlType::String => BqlType::String,
        SqlType::FixedString(i) => BqlType::FixedString(i as u8),
        SqlType::Float32 => BqlType::Float(32),
        SqlType::Float64 => BqlType::Float(64),
        SqlType::Date => BqlType::Date,
        SqlType::DateTime(tz) => match tz {
            Some(tz) => {
                BqlType::DateTimeTz(TimeZoneId(TimeZoneId::calc_offset_of_tz(tz)))
            }
            None => BqlType::DateTime,
        },
        SqlType::DateTime64(id, tz) => {
            BqlType::DateTimeTz(TimeZoneId(TimeZoneId::calc_offset_of_tz(tz)))
        }
        SqlType::Decimal(x, y) => BqlType::Decimal(x, y),
        SqlType::LowCardinality => BqlType::LowCardinalityTinyText,
        SqlType::Ipv4
        | SqlType::Ipv6
        | SqlType::Uuid
        | SqlType::Enum8
        | SqlType::Enum16
        | SqlType::Array => unimplemented!(),
    }
}

// TODO FIXME: The best approach is to unify the server and client block definitions
// to avoid conversions
#[inline]
fn value_ref_to_bytes<'a>(value_ref: &'a ValueRefEnum<'a>) -> &'a [u8] {
    match value_ref {
        ValueRefEnum::String(bytes) => bytes,
        ValueRefEnum::UInt8(u) => u.as_bytes(),
        ValueRefEnum::UInt16(u) => u.as_bytes(),
        ValueRefEnum::UInt32(u) => u.as_bytes(),
        ValueRefEnum::UInt64(u) => u.as_bytes(),
        ValueRefEnum::Int8(i) => i.as_bytes(),
        ValueRefEnum::Int16(i) => i.as_bytes(),
        ValueRefEnum::Int32(i) => i.as_bytes(),
        ValueRefEnum::Int64(i) => i.as_bytes(),
        ValueRefEnum::Date(d) => &d.0,
        ValueRefEnum::DateTime(dt) => &dt.0,
        ValueRefEnum::DateTime64(dt64) => dt64.0.as_bytes(),
        ValueRefEnum::Array8(_)
        | ValueRefEnum::Array16(_)
        | ValueRefEnum::Array32(_)
        | ValueRefEnum::Array64(_)
        | ValueRefEnum::Array128(_)
        | ValueRefEnum::Float32(_)
        | ValueRefEnum::Float64(_)
        | ValueRefEnum::Enum(_)
        | ValueRefEnum::Ip4(_)
        | ValueRefEnum::Ip6(_)
        | ValueRefEnum::Uuid(_)
        | ValueRefEnum::Decimal32(_)
        | ValueRefEnum::Decimal64(_) => unimplemented!(),
    }
}

impl BaseDataBlock {
    pub fn reset(&mut self) {
        self.ncols = 0;
        self.nrows = 0;
        self.columns = vec![];
    }
}

// TODO FIXME: The best approach is to unify the server and client block definitions
// to avoid conversions
impl From<ServerBlock> for BaseDataBlock {
    fn from(b: ServerBlock) -> Self {
        let mut new_blk = BaseDataBlock::default();
        let nrows = b.rows as usize;

        new_blk.ncols = b.columns.len();
        new_blk.nrows = nrows;

        for c in b.columns {
            let btype = sqltype_to_bqltype(c.header.field.get_sqltype());
            let mut data = Vec::with_capacity(4 * 1024);

            for i in 0..nrows {
                // TODO: support NULL
                if let Some(value) = unsafe { c.data.get_at(i as u64) }.into_inner() {
                    let val = value_ref_to_bytes(&value);
                    let val_len = val.len();

                    if btype == BqlType::String {
                        let mut buf = BytesMut::new();
                        buf.write_varint(val_len as u64);
                        data.append(&mut buf.to_vec());
                    }

                    data.reserve(val_len);

                    let len = data.len();
                    let new_len = len + val_len;
                    unsafe {
                        data.set_len(new_len);
                    }

                    data[len..new_len].copy_from_slice(val);
                }
            }
            let chunk = BaseChunk {
                btype,
                size: nrows,
                data,
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            };
            let column = BaseColumn {
                name: c.header.name.as_bytes().to_vec(),
                data: chunk,
            };

            new_blk.columns.push(column);
        }
        new_blk
    }
}

fn arrow_type_to_btype(typ: &DataType) -> BaseRtResult<BqlType> {
    log::debug!("arrow_type_to_btype: {}", typ);
    match typ {
        DataType::UInt8 => Ok(BqlType::UInt(8)),
        DataType::UInt16 => Ok(BqlType::UInt(16)),
        DataType::UInt32 => Ok(BqlType::UInt(32)),
        DataType::UInt64 => Ok(BqlType::UInt(64)),
        DataType::Int8 => Ok(BqlType::Int(8)),
        DataType::Int16 => Ok(BqlType::Int(16)),
        DataType::Int32 => Ok(BqlType::Int(32)),
        DataType::Int64 => Ok(BqlType::Int(64)),
        DataType::Float16 => Ok(BqlType::Float(16)),
        DataType::Float32 => Ok(BqlType::Float(32)),
        DataType::Float64 => Ok(BqlType::Float(64)),
        DataType::Timestamp32(None) => Ok(BqlType::DateTime),
        DataType::Timestamp32(Some(tz)) => Ok(BqlType::DateTimeTz(*tz)),
        DataType::Date16 => Ok(BqlType::Date),
        DataType::Decimal(p, s) => Ok(BqlType::Decimal(*p as u8, *s as u8)),
        DataType::LargeUtf8 => Ok(BqlType::String),
        DataType::FixedSizeBinary(len) => Ok(BqlType::FixedString(*len as u8)),
        _ => Err(BaseRtError::UnsupportedConversionToBqlType),
    }
}

impl TryFrom<RecordBatch> for BaseDataBlock {
    type Error = BaseRtError;

    fn try_from(res: RecordBatch) -> Result<Self, Self::Error> {
        let mut blk = BaseDataBlock::default();
        let sch = res.schema();
        let fields = sch.fields();
        let cols = res.columns();
        let ncols = cols.len();
        blk.ncols = ncols;
        for i in 0..ncols {
            let btype = arrow_type_to_btype(fields[i].data_type())?;
            let name = fields[i].name().as_bytes().to_vec();
            let col = &cols[i];
            let cd = col.data();
            // let array = col.as_any().downcast_ref::<array::Int64Array>().unwrap().values();
            let buf = if matches!(btype, BqlType::String) {
                &col.data().buffers()[1]
            } else {
                &col.data().buffers()[0]
            };
            // log::debug!("cd.get_array_memory_size(): {}", cd.get_array_memory_size());
            let (len_in_bytes, offsets) = if matches!(btype, BqlType::String) {
                let arr = col
                    .as_any()
                    .downcast_ref::<array::LargeStringArray>()
                    .unwrap();
                let ofs = arr
                    .value_offsets()
                    .last()
                    .copied()
                    .ok_or(BaseRtError::FailToUnwrapOpt)?;

                (
                    ofs as usize,
                    Some(arr.value_offsets().iter().map(|o| *o as u32).collect()),
                )
            } else {
                (btype.size_in_usize()? * col.len(), None)
            };
            let data = unsafe {
                std::slice::from_raw_parts(buf.as_ptr(), len_in_bytes).to_vec()
            };
            blk.nrows = col.len(); //FIXME all rows are in same size

            blk.columns.push(BaseColumn {
                name,
                data: BaseChunk {
                    btype,
                    size: col.len(),
                    // data: Vec::from_raw_parts(qcs.data, qclen_bytes, qclen_bytes),
                    // data: Vec::<u8>::with_capacity(qclen_bytes),
                    data,
                    null_map: None,
                    offset_map: offsets,
                    // pub lc_dict_size: usize,
                    lc_dict_data: None,
                },
            });
        }
        Ok(blk)
    }
}

impl BaseColumn {
    // block header:
    // Initialize header from the index.
    //    for (const auto & column : index_block_it->columns)
    //    {
    //        auto type = DataTypeFactory::instance().get(column.type);
    //        header.insert(ColumnWithTypeAndName{ type, column.name });
    //    }
    ///NOTE insert/select needs this kind info to send to client firstly
    pub fn new_block_header(name: Vec<u8>, typ: BqlType, is_nullable: bool) -> Self {
        BaseColumn {
            name,
            data: BaseChunk {
                btype: typ,
                size: 0,
                data: vec![],
                null_map: if is_nullable { Some(vec![]) } else { None },
                offset_map: None,
                lc_dict_data: None,
            },
        }
    }

    #[inline]
    pub fn get_name<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.name) }
    }
}

pub trait BaseServerConn {
    fn get_query_id(&self) -> &str;
    fn set_query_id(&mut self, query_id: String);
    fn get_db(&self) -> &str;
    fn set_db(&mut self, db: String);
    fn set_compressed(&mut self, is_compressed: bool);
    fn is_compressed(&self) -> bool;
}

pub trait BaseWriteAware {
    fn write_varint(&mut self, value: u64);
    fn write_varbytes(&mut self, value: &[u8]);
}

impl BaseWriteAware for BytesMut {
    #[inline(always)]
    fn write_varint(&mut self, value: u64) {
        self.reserve(10); //FIXME
        let buf =
            unsafe { slice::from_raw_parts_mut(self.as_mut_ptr().add(self.len()), 10) };
        let vi_len = encode_varint64(value, buf);
        unsafe {
            self.advance_mut(vi_len);
        }
    }

    #[inline(always)]
    fn write_varbytes(&mut self, value: &[u8]) {
        let len = value.len();
        self.reserve(10 + len); //FIXME
        self.write_varint(len as u64);
        // value.as_bytes().copy_to_slice()
        unsafe {
            copy_nonoverlapping(value.as_ptr(), self.as_mut_ptr().add(self.len()), len);
            self.advance_mut(len);
        }
    }
}
