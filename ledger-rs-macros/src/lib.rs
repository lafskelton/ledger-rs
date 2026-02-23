use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Expr, Fields, LitInt, Meta, Token, parse_macro_input, punctuated::Punctuated,
};

const DEFAULT_ROWS_PER_PAGE: u32 = 4096;

#[proc_macro_attribute]
pub fn ledger(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    //
    let mut rows_per_page: u32 = DEFAULT_ROWS_PER_PAGE;
    //
    for meta in args {
        match meta {
            Meta::NameValue(nv) => {
                let name = match nv.path.get_ident() {
                    Some(v) => v,
                    None => {
                        panic!("bad attr");
                    }
                }
                .to_string();
                //
                match name.as_str() {
                    "page_size" => {
                        match nv.value {
                            Expr::Lit(v) => {
                                let Some(rows_per_page_str) = v.lit.span().source_text() else {
                                    continue;
                                };
                                //
                                rows_per_page = u32::from_str_radix(&rows_per_page_str, 10).unwrap()
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        };
    }

    let input = parse_macro_input!(item as DeriveInput);
    let struct_name = &input.ident;
    let archived_struct_name = quote::format_ident!("Archived{}", struct_name);

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => panic!("#[ledger_row] only supports structs with named fields"),
        },
        _ => panic!("#[ledger_row] can only be applied to structs"),
    };

    let mut new_struct_fields = Vec::new();
    let mut generated_methods = Vec::new();

    // Vectors to build our automated `new(...)` constructor and default trait initialization
    let mut constructor_params = Vec::new();
    let mut constructor_setup = Vec::new();
    let mut constructor_init = Vec::new();
    let mut default_init = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        let mut max_len_val: Option<usize> = None;

        for attr in &field.attrs {
            if attr.path().is_ident("max_len") {
                let lit: LitInt = attr
                    .parse_args()
                    .expect("max_len requires an integer, e.g., #[max_len(32)]");
                max_len_val = Some(lit.base10_parse::<usize>().unwrap());
            }
        }

        if let Some(max_len) = max_len_val {
            let is_string = if let syn::Type::Path(type_path) = field_type {
                type_path.path.is_ident("String")
            } else {
                false
            };

            if !is_string {
                panic!(
                    "Field '{}' has #[max_len] but is not a `String`. Arrays like [String; N] cannot use this attribute.",
                    field_name
                );
            }

            let len_field_name = format_ident!("{}_len", field_name);
            let setter_name = format_ident!("set_{}", field_name);

            // 1. Struct Fields
            new_struct_fields.push(quote! {
                #len_field_name: u8,
                #field_name: [u8; #max_len]
            });

            // 2. Constructor Logic (e.g., parameter: `device_id: &str`)
            constructor_params.push(quote! { #field_name: &str });

            let bytes_ident = format_ident!("{}_bytes", field_name);
            let buf_ident = format_ident!("{}_buf", field_name);
            let len_ident = format_ident!("{}_len_val", field_name);

            constructor_setup.push(quote! {
                let #bytes_ident = #field_name.as_bytes();
                let mut #buf_ident = [b' '; #max_len];
                let #len_ident = #bytes_ident.len().min(#max_len);
                #buf_ident[..#len_ident].copy_from_slice(&#bytes_ident[..#len_ident]);
            });

            constructor_init.push(quote! {
                #len_field_name: #len_ident as u8,
                #field_name: #buf_ident
            });

            default_init.push(quote! {
                #len_field_name: 0,
                #field_name: [b' '; #max_len]
            });

            // 3. Getters & Setters
            generated_methods.push(quote! {
                pub fn #field_name<'a>(&'a self) -> Result<&'a str, ::ledger_rs::utils::DatastoreError> {
                    std::str::from_utf8(&self.#field_name[0..self.#len_field_name as usize])
                        .map_err(|e| ::ledger_rs::utils::DatastoreError::Error(e.to_string()))
                }

                pub fn #setter_name(&mut self, val: &str) -> Result<(), ::ledger_rs::utils::DatastoreError> {
                    let bytes = val.as_bytes();
                    let byte_len = bytes.len();

                    if byte_len > #max_len || byte_len == 0 {
                        return Err(::ledger_rs::utils::DatastoreError::Error(
                            concat!(stringify!(#field_name), " length out of bounds").into()
                        ));
                    }

                    self.#len_field_name = byte_len as u8;
                    self.#field_name[0..byte_len].copy_from_slice(bytes);
                    Ok(())
                }
            });
        } else {
            let mut is_mapped = false;
            let mut mapped_type = quote! { #field_type };

            if let syn::Type::Path(type_path) = field_type {
                if let Some(ident) = type_path.path.get_ident() {
                    match ident.to_string().as_str() {
                        "u16" => {
                            mapped_type = quote! { ::rkyv::rend::u16_le };
                            is_mapped = true;
                        }
                        "u32" => {
                            mapped_type = quote! { ::rkyv::rend::u32_le };
                            is_mapped = true;
                        }
                        "u64" => {
                            mapped_type = quote! { ::rkyv::rend::u64_le };
                            is_mapped = true;
                        }
                        "u128" => {
                            mapped_type = quote! { ::rkyv::rend::u128_le };
                            is_mapped = true;
                        }
                        "i16" => {
                            mapped_type = quote! { ::rkyv::rend::i16_le };
                            is_mapped = true;
                        }
                        "i32" => {
                            mapped_type = quote! { ::rkyv::rend::i32_le };
                            is_mapped = true;
                        }
                        "i64" => {
                            mapped_type = quote! { ::rkyv::rend::i64_le };
                            is_mapped = true;
                        }
                        "i128" => {
                            mapped_type = quote! { ::rkyv::rend::i128_le };
                            is_mapped = true;
                        }
                        "f32" => {
                            mapped_type = quote! { ::rkyv::rend::f32_le };
                            is_mapped = true;
                        }
                        "f64" => {
                            mapped_type = quote! { ::rkyv::rend::f64_le };
                            is_mapped = true;
                        }
                        "usize" => {
                            mapped_type = quote! { ::rkyv::rend::u64_le };
                            is_mapped = true;
                        }
                        _ => {}
                    }
                }
                //
            } else if let syn::Type::Array(type_array) = field_type {
                //
                if let syn::Type::Path(elem_path) = &*type_array.elem {
                    if let Some(ident) = elem_path.path.get_ident() {
                        let ident_str = ident.to_string();

                        let allowed_array_types = [
                            "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64",
                            "i128", "u128", "usize",
                        ];

                        if !allowed_array_types.contains(&ident_str.as_str()) {
                            panic!(
                                "Field '{}' has unsupported array type '[{}; ...]'. Only primitive arrays (e.g., [u8; N]) are allowed.",
                                field_name, ident_str
                            );
                        }
                    } else {
                        panic!("Field '{}' has an invalid array element type.", field_name);
                    }
                } else {
                    panic!(
                        "Field '{}' has a complex array type, which is not supported.",
                        field_name
                    );
                }
            }

            new_struct_fields.push(quote! {
                 #field_name: #mapped_type
            });

            // Ask the user for the standard type (e.g., u64)
            constructor_params.push(quote! { #field_name: #field_type });

            if is_mapped {
                // Secretly wrap it in the _le equivalent
                constructor_init.push(quote! {
                    #field_name: <#mapped_type>::from_native(#field_name)
                });
                default_init.push(quote! {
                    #field_name: <#mapped_type>::from_native(Default::default())
                });

                // --- NEW GETTERS AND SETTERS FOR MAPPED TYPES ---
                let setter_name = format_ident!("set_{}", field_name);
                generated_methods.push(quote! {
                    pub fn #field_name(&self) -> #field_type {
                        self.#field_name.to_native()
                    }

                    pub fn #setter_name(&mut self, val: #field_type) {
                        self.#field_name = <#mapped_type>::from_native(val);
                    }
                });
                // ------------------------------------------------
            } else {
                constructor_init.push(quote! {
                    #field_name
                });
                default_init.push(quote! {
                    #field_name: Default::default()
                });
            }

            // if is_mapped {
            //     // Secretly wrap it in the _le equivalent
            //     constructor_init.push(quote! {
            //         #field_name: <#mapped_type>::from_native(#field_name)
            //     });
            //     default_init.push(quote! {
            //         #field_name: <#mapped_type>::from_native(Default::default())
            //     });
            // } else {
            //     constructor_init.push(quote! {
            //         #field_name
            //     });
            //     default_init.push(quote! {
            //         #field_name: Default::default()
            //     });
            // }
        }
    }

    let ledger_alias_name = format_ident!("{}Ledger", struct_name);

    let expanded = quote! {
        #[derive(
            ::bytecheck::CheckBytes,
            ::rkyv::Archive,
            ::rkyv::Deserialize,
            ::rkyv::Portable,
            ::rkyv::Serialize,
            Clone, Debug, PartialEq
        )]
        #[rkyv(compare(PartialEq))]
        #[repr(C)]
        pub struct #struct_name {
            #(#new_struct_fields,)*
             _pad: [u8; 2],
        }

        unsafe impl ::rkyv::traits::NoUndef for #struct_name {}
        unsafe impl ::rkyv::traits::NoUndef for #archived_struct_name {}

        pub type #ledger_alias_name = ::ledger_rs::ledger::DataLedgerStore<
            #struct_name,
            { <#struct_name as ::ledger_rs::page::PageSchema>::PAGE_SZ },
            { <#struct_name as ::ledger_rs::page::PageSchema>::ROWS_PER_PAGE }
        >;

        impl #struct_name {
            pub fn new(#(#constructor_params),*) -> Self {
                #(#constructor_setup)*

                Self {
                    #(#constructor_init,)*
                    _pad: [0; 2],
                }
            }

            // USE default_init HERE
            pub fn default() -> Self {
                Self {
                    #(#default_init,)* _pad: [0; 2],
                }
            }

            pub fn create_ledger(
                folder_path: &::std::path::Path,
                ledger_name: &str,
                ledger_description: &str,
            ) -> Result<#ledger_alias_name, ::ledger_rs::utils::DatastoreError>
            {
                ::ledger_rs::ledger::DataLedgerStore::<
                    Self,
                    { Self::PAGE_SZ },
                    { Self::ROWS_PER_PAGE }
                >::open(
                    folder_path,
                    ::ledger_rs::header::LedgerName::from(ledger_name),
                    ::ledger_rs::header::LedgerDescription::new(ledger_description),
                )
            }

            #(#generated_methods)*
        }

        impl ::ledger_rs::page::PageSchema for #struct_name {
            const ROWS_PER_PAGE: usize = #rows_per_page as usize;
            const PAGE_SZ: usize = ::ledger_rs::page::page_sz::<Self>() as usize;

            fn to_bytes(&self) -> Result<::rkyv::util::AlignedVec, ::rkyv::rancor::Error> {
                ::rkyv::to_bytes::<::rkyv::rancor::Error>(self)
            }

            fn from_bytes(f: &::rkyv::util::AlignedVec) -> Result<Self, ::rkyv::rancor::Error> {
                ::rkyv::from_bytes::<Self, ::rkyv::rancor::Error>(f)
            }

            fn deleted_row(_page_row_n: usize) -> &'static [u8] {
                &[]
            }
        }
    };

    TokenStream::from(expanded)
}
