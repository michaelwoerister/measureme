//! A string table implementation with a tree-like encoding.
//!
//! Each entry in the table represents a string and encoded is a list of
//! components where each component can either be
//!
//! 1. a TAG_STR_VAL that contains actual string content,
//! 2. a TAG_STR_REF that contains a reference to another entry, or
//! 3. a TAG_TERMINATOR which marks the end of a component list.
//!
//! The string content of an entry is defined as the concatenation of the
//! content of its components. The content of a `TAG_STR_VAL` is its actual
//! UTF-8 bytes. The content of a `TAG_STR_REF` is the contents of the entry
//! it references.
//!
//! Each string is referred to via a `StringId`. `StringId`s may be generated in two ways:
//!     1. Calling `StringTable::alloc()` which returns the `StringId` for the allocated string.
//!     2. Calling `StringTable::alloc_with_reserved_id()` and `StringId::reserved()`.
//!
//! Reserved strings allow you to deduplicate strings by allocating a string once and then referring
//! to it by id over and over. This is a useful trick for strings which are recorded many times and
//! it can significantly reduce the size of profile trace files.
//!
//! `StringId`s are partitioned according to type:
//!
//! > [0 .. MAX_PRE_RESERVED_STRING_ID, METADATA_STRING_ID, .. ]
//!
//! From `0` to `MAX_PRE_RESERVED_STRING_ID` are the allowed values for reserved strings.
//! After `MAX_PRE_RESERVED_STRING_ID`, there is one string id (`METADATA_STRING_ID`) which is used
//! internally by `measureme` to record additional metadata about the profiling session.
//! After `METADATA_STRING_ID` are all other `StringId` values.

use crate::file_header::{
    read_file_header, strip_file_header, write_file_header, CURRENT_FILE_FORMAT_VERSION,
    FILE_MAGIC_STRINGTABLE_DATA, FILE_MAGIC_STRINGTABLE_INDEX,
};
use crate::serialization::{Addr, SerializationSink};
use byteorder::{ByteOrder, LittleEndian};
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::error::Error;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// A `StringId` is used to identify a string in the `StringTable`.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
#[repr(C)]
pub struct StringId(u32);

impl StringId {
    #[inline]
    pub fn reserved(id: u32) -> StringId {
        assert!(id == id & STRING_ID_MASK);
        StringId(id)
    }
}

// Tags for the binary encoding of strings

/// Marks the end of a string component list.
const TERMINATOR: u8 = 0xFF;
const UTF8_CONTINUATION_MASK: u8 = 0b1100_0000;
const UTF8_CONTINUATION_BYTE: u8 = 0b1000_0000;

const MAX_STRING_ID: u32 = 0x3FFF_FFFF;
const STRING_ID_MASK: u32 = 0x3FFF_FFFF;

/// The maximum id value a prereserved string may be.
const MAX_PRE_RESERVED_STRING_ID: u32 = MAX_STRING_ID / 2;
/// The id of the profile metadata string entry.
pub(crate) const METADATA_STRING_ID: u32 = MAX_PRE_RESERVED_STRING_ID + 1;


/// Write-only version of the string table
pub struct StringTableBuilder<S: SerializationSink> {
    data_sink: Arc<S>,
    index_sink: Arc<S>,
    id_counter: AtomicU32, // initialized to METADATA_STRING_ID + 1
}

/// Anything that implements `SerializableString` can be written to a
/// `StringTable`.
pub trait SerializableString {
    fn serialized_size(&self) -> usize;
    fn serialize(&self, bytes: &mut [u8]);
}

impl SerializableString for str {
    #[inline]
    fn serialized_size(&self) -> usize {
        self.len() + // actual bytes
        1 // terminator
    }

    #[inline]
    fn serialize(&self, bytes: &mut [u8]) {
        let last_byte_index = bytes.len() - 1;
        bytes[0..last_byte_index].copy_from_slice(self.as_bytes());
        bytes[last_byte_index] = TERMINATOR;
    }
}

/// A single component of a string. Used for building composite table entries.
pub enum StringComponent<'s> {
    Value(&'s str),
    Ref(StringId),
}

impl<'s> StringComponent<'s> {
    #[inline]
    fn serialized_size(&self) -> usize {
        match *self {
            StringComponent::Value(s) => s.len(),
            StringComponent::Ref(_) => 4,
        }
    }

    #[inline]
    fn serialize<'b>(&self, bytes: &'b mut [u8]) -> &'b mut [u8] {
        match *self {
            StringComponent::Value(s) => {
                bytes[..s.len()].copy_from_slice(s.as_bytes());
                &mut bytes[s.len() ..]
            }
            StringComponent::Ref(string_id) => {
                debug_assert_eq!(string_id.0, string_id.0 & STRING_ID_MASK);
                LittleEndian::write_u32(&mut bytes[0..4], string_id.0);
                &mut bytes[4 ..]
            }
        }
    }
}

impl<'a> SerializableString for [StringComponent<'a>] {
    #[inline]
    fn serialized_size(&self) -> usize {
        self.iter().map(|c| c.serialized_size()).sum::<usize>() + // size of components
        1 // terminator
    }

    #[inline]
    fn serialize(&self, mut bytes: &mut [u8]) {
        debug_assert!(bytes.len() == self.serialized_size());
        for component in self.iter() {
            bytes = component.serialize(bytes);
        }

        debug_assert!(bytes.len() == 1);
        bytes[0] = TERMINATOR;
    }
}

macro_rules! impl_serializable_string_for_fixed_size {
    ($n:expr) => (
        impl<'a> SerializableString for [StringComponent<'a>; $n] {
            #[inline(always)]
            fn serialized_size(&self) -> usize {
                (&self[..]).serialized_size()
            }

            #[inline(always)]
            fn serialize(&self, bytes: &mut [u8]) {
                (&self[..]).serialize(bytes);
            }
        }
    )
}

impl_serializable_string_for_fixed_size!(0);
impl_serializable_string_for_fixed_size!(1);
impl_serializable_string_for_fixed_size!(2);
impl_serializable_string_for_fixed_size!(3);
impl_serializable_string_for_fixed_size!(4);
impl_serializable_string_for_fixed_size!(5);
impl_serializable_string_for_fixed_size!(6);
impl_serializable_string_for_fixed_size!(7);
impl_serializable_string_for_fixed_size!(8);
impl_serializable_string_for_fixed_size!(9);
impl_serializable_string_for_fixed_size!(10);
impl_serializable_string_for_fixed_size!(11);
impl_serializable_string_for_fixed_size!(12);
impl_serializable_string_for_fixed_size!(13);
impl_serializable_string_for_fixed_size!(14);
impl_serializable_string_for_fixed_size!(15);
impl_serializable_string_for_fixed_size!(16);

fn serialize_index_entry<S: SerializationSink>(sink: &S, id: StringId, addr: Addr) {
    sink.write_atomic(8, |bytes| {
        LittleEndian::write_u32(&mut bytes[0..4], id.0);
        LittleEndian::write_u32(&mut bytes[4..8], addr.0);
    });
}

fn deserialize_index_entry(bytes: &[u8]) -> (StringId, Addr) {
    (
        StringId(LittleEndian::read_u32(&bytes[0..4])),
        Addr(LittleEndian::read_u32(&bytes[4..8])),
    )
}

impl<S: SerializationSink> StringTableBuilder<S> {
    pub fn new(data_sink: Arc<S>, index_sink: Arc<S>) -> StringTableBuilder<S> {
        // The first thing in every file we generate must be the file header.
        write_file_header(&*data_sink, FILE_MAGIC_STRINGTABLE_DATA);
        write_file_header(&*index_sink, FILE_MAGIC_STRINGTABLE_INDEX);

        StringTableBuilder {
            data_sink,
            index_sink,
            id_counter: AtomicU32::new(METADATA_STRING_ID + 1),
        }
    }

    #[inline]
    pub fn alloc_with_reserved_id<STR: SerializableString + ?Sized>(
        &self,
        id: StringId,
        s: &STR,
    ) -> StringId {
        assert!(id.0 <= MAX_PRE_RESERVED_STRING_ID);
        self.alloc_unchecked(id, s);
        id
    }

    pub(crate) fn alloc_metadata<STR: SerializableString + ?Sized>(&self, s: &STR) -> StringId {
        let id = StringId(METADATA_STRING_ID);
        self.alloc_unchecked(id, s);
        id
    }

    #[inline]
    pub fn alloc<STR: SerializableString + ?Sized>(&self, s: &STR) -> StringId {
        let id = StringId(self.id_counter.fetch_add(1, Ordering::SeqCst));
        assert_eq!(id.0, id.0 & STRING_ID_MASK);
        debug_assert!(id.0 > METADATA_STRING_ID);
        self.alloc_unchecked(id, s);
        id
    }

    #[inline]
    fn alloc_unchecked<STR: SerializableString + ?Sized>(&self, id: StringId, s: &STR) {
        let size_in_bytes = s.serialized_size();
        let addr = self.data_sink.write_atomic(size_in_bytes, |mem| {
            s.serialize(mem);
        });

        serialize_index_entry(&*self.index_sink, id, addr);
    }
}

#[derive(Copy, Clone)]
pub struct StringRef<'st> {
    id: StringId,
    table: &'st StringTable,
}

impl<'st> StringRef<'st> {
    pub fn to_string(&self) -> Cow<'st, str> {
        let mut output = String::new();
        self.write_to_string(&mut output);
        Cow::from(output)
    }

    pub fn write_to_string(&self, output: &mut String) {
        let addr = self.table.index[&self.id];

        let mut pos = addr.as_usize();

        loop {
            let byte = self.table.string_data[pos];

            if byte == TERMINATOR {
                return
            } else if (byte & UTF8_CONTINUATION_MASK) == UTF8_CONTINUATION_BYTE {
                // This is a string-id
                let id = LittleEndian::read_u32(&self.table.string_data[pos .. pos + 4]);

                let string_ref = StringRef {
                    id: StringId(id),
                    table: self.table
                };

                string_ref.write_to_string(output);

                pos += 4;
            } else {
                while let Some((c, len)) = decode_utf8_char(&self.table.string_data[pos ..]) {
                    output.push(c);
                    pos += len;
                }
            }
        }
    }
}

fn decode_utf8_char(bytes: &[u8]) -> Option<(char, usize)> {
    use std::convert::TryFrom;

    let first_byte = bytes[0] as u32;

    let (codepoint, len) = if (first_byte & 0b1000_0000) == 0 {
        (first_byte, 1)
    } else if (first_byte & 0b1110_0000) == 0b1100_0000 {
        let bits0 = first_byte & 0b0001_1111;
        let bits1 = (bytes[1] & 0b0011_1111) as u32;

        (bits0 << 6 | bits1, 2)
    } else if (first_byte & 0b1111_0000) == 0b1110_0000 {
        let bits0 = first_byte & 0b0001_1111;
        let bits1 = (bytes[1] & 0b0011_1111) as u32;
        let bits2 = (bytes[2] & 0b0011_1111) as u32;

        ((bits0 << 12) | (bits1 << 6) | bits2, 3)
    } else {
        return None
    };

    match char::try_from(codepoint) {
        Ok(c) => {
            debug_assert!({
                let test_bytes = &mut [0u8; 8];
                c.encode_utf8(test_bytes);

                &test_bytes[..len] == &bytes[..len]
            });

            Some((c, len))
        }
        Err(e) => {
            panic!("StringTable: Encountered invalid UTF8 char: {:?}", e);
        }
    }
}

/// Read-only version of the string table
pub struct StringTable {
    // TODO: Replace with something lazy
    string_data: Vec<u8>,
    index: FxHashMap<StringId, Addr>,
}

impl StringTable {
    pub fn new(string_data: Vec<u8>, index_data: Vec<u8>) -> Result<StringTable, Box<dyn Error>> {
        let string_data_format = read_file_header(&string_data, FILE_MAGIC_STRINGTABLE_DATA)?;
        let index_data_format = read_file_header(&index_data, FILE_MAGIC_STRINGTABLE_INDEX)?;

        if string_data_format != index_data_format {
            Err("Mismatch between StringTable DATA and INDEX format version")?;
        }

        if string_data_format != CURRENT_FILE_FORMAT_VERSION {
            Err(format!(
                "StringTable file format version '{}' is not supported
                         by this version of `measureme`.",
                string_data_format
            ))?;
        }

        assert!(index_data.len() % 8 == 0);
        let index: FxHashMap<_, _> = strip_file_header(&index_data)
            .chunks(8)
            .map(deserialize_index_entry)
            .collect();

        Ok(StringTable { string_data, index })
    }

    #[inline]
    pub fn get<'a>(&'a self, id: StringId) -> StringRef<'a> {
        StringRef { id, table: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_strings() {
        use crate::serialization::ByteVecSink;

        let data_sink = Arc::new(ByteVecSink::new());
        let index_sink = Arc::new(ByteVecSink::new());

        let expected_strings = &[
            "abc",
            "",
            "xyz",
            "g2h9284hgjv282y32983849&(*^&YIJ#R)(F83 f 23 2g4 35g5y",
            "",
            "",
            "g2h9284hgjv282y32983849&35g5y",
        ];

        let mut string_ids = vec![];

        {
            let builder = StringTableBuilder::new(data_sink.clone(), index_sink.clone());

            for &s in expected_strings {
                string_ids.push(builder.alloc(s));
            }
        }

        let data_bytes = Arc::try_unwrap(data_sink).unwrap().into_bytes();
        let index_bytes = Arc::try_unwrap(index_sink).unwrap().into_bytes();

        let string_table = StringTable::new(data_bytes, index_bytes).unwrap();

        for (&id, &expected_string) in string_ids.iter().zip(expected_strings.iter()) {
            let str_ref = string_table.get(id);

            assert_eq!(str_ref.to_string(), expected_string);

            let mut write_to = String::new();
            str_ref.write_to_string(&mut write_to);
            assert_eq!(str_ref.to_string(), write_to);
        }
    }

    #[test]
    fn composite_string() {
        use crate::serialization::ByteVecSink;

        let data_sink = Arc::new(ByteVecSink::new());
        let index_sink = Arc::new(ByteVecSink::new());

        let expected_strings = &[
            "abc", // 0
            "abcabc", // 1
            "abcabcabc", // 2
            "abcabcabc", // 3
            "abcabcabc", // 4
            "abcabcabcabc", // 5
            "xxabcuuuabcqqq", // 6
            "xxxxxx", // 7
        ];

        let mut string_ids = vec![];

        {
            let builder = StringTableBuilder::new(data_sink.clone(), index_sink.clone());

            let r = |id| StringComponent::Ref(id);
            let v = |s| StringComponent::Value(s);

            string_ids.push(builder.alloc("abc")); // 0
            string_ids.push(builder.alloc(&[r(string_ids[0]), r(string_ids[0])])); // 1
            string_ids.push(builder.alloc(&[r(string_ids[0]), r(string_ids[0]), r(string_ids[0])])); // 2
            string_ids.push(builder.alloc(&[r(string_ids[1]), r(string_ids[0])])); // 3
            string_ids.push(builder.alloc(&[r(string_ids[0]), r(string_ids[1])])); // 4
            string_ids.push(builder.alloc(&[r(string_ids[1]), r(string_ids[1])])); // 5
            string_ids.push(builder.alloc(&[v("xx"), r(string_ids[1]), v("uuu"), r(string_ids[1]), v("qqq"),])); // 6
        }

        let data_bytes = Arc::try_unwrap(data_sink).unwrap().into_bytes();
        let index_bytes = Arc::try_unwrap(index_sink).unwrap().into_bytes();

        let string_table = StringTable::new(data_bytes, index_bytes).unwrap();

        for (&id, &expected_string) in string_ids.iter().zip(expected_strings.iter()) {
            let str_ref = string_table.get(id);

            assert_eq!(str_ref.to_string(), expected_string);

            let mut write_to = String::new();
            str_ref.write_to_string(&mut write_to);
            assert_eq!(str_ref.to_string(), write_to);
        }
    }
}
