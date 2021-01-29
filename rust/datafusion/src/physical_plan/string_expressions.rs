// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! String expressions

use crate::error::{DataFusionError, Result};
use arrow::array::{
    Array, ArrayRef, GenericStringArray, Int64Array, StringArray, StringBuilder,
    StringOffsetSizeTrait,
};

macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => Err(DataFusionError::Internal("failed to downcast".to_string())),
            })
    }};
}

/// concatenate string columns together.
pub fn concatenate(args: &[ArrayRef]) -> Result<StringArray> {
    // downcast all arguments to strings
    let args = downcast_vec!(args, StringArray).collect::<Result<Vec<&StringArray>>>()?;
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "Concatenate was called with 0 arguments. It requires at least one."
                .to_string(),
        ));
    }

    let mut builder = StringBuilder::new(args.len());
    // for each entry in the array
    for index in 0..args[0].len() {
        let mut owned_string: String = "".to_owned();

        // if any is null, the result is null
        let mut is_null = false;
        for arg in &args {
            if arg.is_null(index) {
                is_null = true;
                break; // short-circuit as we already know the result
            } else {
                owned_string.push_str(&arg.value(index));
            }
        }
        if is_null {
            builder.append_null()?;
        } else {
            builder.append_value(&owned_string)?;
        }
    }
    Ok(builder.finish())
}

/// Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).
pub fn lpad<T: StringOffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<GenericStringArray<T>> {
    match args.len() {
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let length_array: &Int64Array = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast length to Int64Array".to_string(),
                    )
                })?;

            Ok(string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;
                            if length == 0 {
                                "".to_string()
                            } else if length < x.len() {
                                x[..length].to_string()
                            } else {
                                let mut s = x.to_string();
                                s.insert_str(0, " ".repeat(length - x.len()).as_str());
                                s
                            }
                        })
                    }
                })
                .collect())
        }
        3 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let length_array: &Int64Array =
                args[1].as_any().downcast_ref::<Int64Array>().unwrap();

            let fill_array: &GenericStringArray<T> = args[2]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            Ok(string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) || fill_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;
                            let fill_chars =
                                fill_array.value(i).chars().collect::<Vec<char>>();
                            if length == 0 {
                                "".to_string()
                            } else if length < x.len() {
                                x[..length].to_string()
                            } else if fill_chars.is_empty() {
                                x.to_string()
                            } else {
                                let mut s = x.to_string();
                                let mut char_vector =
                                    Vec::<char>::with_capacity(length - x.len());
                                for l in 0..length - x.len() {
                                    char_vector.push(
                                        *fill_chars.get(l % fill_chars.len()).unwrap(),
                                    );
                                }
                                s.insert_str(
                                    0,
                                    char_vector.iter().collect::<String>().as_str(),
                                );
                                s
                            }
                        })
                    }
                })
                .collect())
        }
        other => Err(DataFusionError::Internal(format!(
            "lpad was called with {} arguments. It requires 2 or 3.",
            other
        ))),
    }
}

macro_rules! string_unary_function {
    ($NAME:ident, $FUNC:ident) => {
        /// string function that accepts Utf8 or LargeUtf8 and returns Utf8 or LargeUtf8
        pub fn $NAME<T: StringOffsetSizeTrait>(
            args: &[ArrayRef],
        ) -> Result<GenericStringArray<T>> {
            let array = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();
            // first map is the iterator, second is for the `Option<_>`
            Ok(array.iter().map(|x| x.map(|x| x.$FUNC())).collect())
        }
    };
}

string_unary_function!(lower, to_ascii_lowercase);
string_unary_function!(upper, to_ascii_uppercase);
string_unary_function!(trim, trim);
string_unary_function!(ltrim, trim_start);
string_unary_function!(rtrim, trim_end);
