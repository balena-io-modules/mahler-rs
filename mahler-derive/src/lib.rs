//! Derive macros for the mahler automated job orchestration library.
//!
//! This crate provides procedural macros to simplify working with mahler models and state management.
//!
//! ## Dependencies
//!
//! This crate requires `serde` to be available in your project since the generated code
//! uses serde's `Serialize` and `Deserialize` derives. Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! serde = { version = "1.0", features = ["derive"] }
//! ```
//!
//! # State Derive Macro
//!
//! The `#[derive(State)]` macro automatically implements the
//! [State](`mahler::State`) trait and generates a Target type.
//!
//! ```rust
//! use mahler::State;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(State, Serialize, Deserialize, Debug, Clone)]
//! struct Service {
//!     name: String,
//!     #[serde(skip_serializing_if = "Option::is_none")]
//!     image: Option<String>,
//!
//!     // This field won't appear in ServiceTarget
//!     #[mahler(internal)]
//!     container_id: Option<String>,
//! }
//! ```
//!
//! This generates:
//! - A `ServiceTarget` struct with the same fields except those marked `#[mahler(internal)]`
//! - An implementation of `State` for `Service`
//! - All serde attributes are preserved on both types

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Error, Field, Fields, Meta, Result};

type NamedTargetFields = Vec<proc_macro2::TokenStream>;
type UnnamedTargetFields = Vec<proc_macro2::TokenStream>;

fn process_named_fields(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
) -> Result<NamedTargetFields> {
    let mut target_fields = NamedTargetFields::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_vis = &field.vis;

        // Check if field is marked as internal
        let is_internal = has_mahler_internal_attribute(&field.attrs)?;

        if is_internal {
            // Internal field: skip from target struct
        } else {
            // External field: appears in target struct

            // Copy all non-mahler attributes to target field
            let target_attrs = filter_field_attributes(&field.attrs);

            // Use ::Target consistently for all field types
            target_fields.push(quote! {
                #(#target_attrs)*
                #field_vis #field_name: <#field_type as ::mahler::state::State>::Target
            });
        }
    }

    Ok(target_fields)
}

fn process_unnamed_fields(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
) -> Result<UnnamedTargetFields> {
    let mut target_fields = UnnamedTargetFields::new();

    for field in fields.iter() {
        let field_type = &field.ty;

        // Check if field is marked as internal
        let is_internal = has_mahler_internal_attribute(&field.attrs)?;

        if is_internal {
            return Err(Error::new_spanned(
                field,
                "#[mahler(internal)] is only supported on named struct fields, not tuple struct fields"
            ));
        } else {
            // External field: appears in both structs
            // Copy all non-mahler attributes to target field
            let target_attrs = filter_field_attributes(&field.attrs);

            target_fields.push(quote! {
                #(#target_attrs)*
                <#field_type as ::mahler::state::State>::Target
            });
        }
    }

    Ok(target_fields)
}

fn has_mahler_internal_attribute(attrs: &[Attribute]) -> Result<bool> {
    for attr in attrs {
        if attr.path().is_ident("mahler") {
            match &attr.meta {
                Meta::List(meta_list) => {
                    // Parse #[mahler(internal)]
                    let tokens = &meta_list.tokens;
                    if tokens.to_string().trim() == "internal" {
                        return Ok(true);
                    }
                }
                Meta::Path(_) => {
                    // Just #[mahler] - not what we want
                }
                Meta::NameValue(_) => {
                    // #[mahler = "value"] - not what we want
                }
            }
        }
    }
    Ok(false)
}

fn filter_attributes(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| {
            // Ignore any `mahler` attributes
            !attr.path().is_ident("mahler")
        })
        .collect()
}

fn filter_field_attributes(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| {
            // Propagate all attributes except mahler-specific ones
            !attr.path().is_ident("mahler")
        })
        .collect()
}

fn expand_model_derive(input: DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    let target_name = format_ident!("{}Target", struct_name);

    // Extract generics information
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Use `mahler::state` path since that's the published crate
    let model_path = quote! { ::mahler::state::State };

    match input.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields) => {
                    // Handle named fields (struct { field: Type })
                    let target_fields = process_named_fields(&fields.named)?;

                    // Preserve all attributes from the original struct except mahler specific ones
                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        // Generate the target struct with sensible default derives
                        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
                        #(#struct_attrs)*
                        pub struct #target_name #generics #where_clause {
                            #(#target_fields,)*
                        }

                        // Implement the State trait
                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;
                        }
                    };

                    Ok(expanded.into())
                }
                Fields::Unnamed(fields) => {
                    // Handle tuple structs (struct(Type1, Type2))
                    let target_fields = process_unnamed_fields(&fields.unnamed)?;

                    // Preserve all attributes from the original struct except derive
                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        // Generate the target struct with sensible default derives
                        #(#struct_attrs)*
                        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
                        pub struct #target_name #generics (#(#target_fields,)*) #where_clause;

                        // Implement the State trait
                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;
                        }
                    };

                    Ok(expanded.into())
                }
                Fields::Unit => {
                    // Handle unit structs (struct UnitStruct;)
                    // Preserve all attributes from the original struct except derive
                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        // Generate the target struct (same as original for unit structs) with sensible default derives
                        #(#struct_attrs)*
                        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
                        pub struct #target_name #generics #where_clause;

                        // Implement the State trait
                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;
                        }
                    };

                    Ok(expanded.into())
                }
            }
        }
        Data::Enum(_) => {
            // For enums, Target = Self since enums don't have internal fields
            let expanded = quote! {
                // Implement the State trait for enums
                impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                    type Target = Self;
                }
            };

            Ok(expanded.into())
        }
        _ => Err(Error::new_spanned(
            struct_name,
            "State can only be derived for structs and enums",
        )),
    }
}

/// Derives the `State` trait for a struct or enum, generating a compatible target type.
///
/// For structs, this macro creates a `{StructName}Target` type that excludes fields marked
/// with `#[mahler(internal)]`. For enums, the target type is the same as the original enum
/// since enums don't have internal fields.
///
/// # Attributes
///
/// - `#[mahler(internal)]` - Marks a struct field as internal-only, excluding it from the target type.
///   Only supported on named struct fields, not tuple struct, unit struct, or enum fields.
///
/// # Example
///
/// ```rust
/// use mahler::State;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(State, Serialize, Deserialize)]
/// struct DatabaseConfig {
///     host: String,
///     port: u16,
///
///     #[serde(skip_serializing_if = "Option::is_none")]
///     database: Option<String>,
///
///     // Internal field - not part of target
///     #[mahler(internal)]
///     connection_pool: Option<String>,
/// }
///
/// #[derive(State, Serialize, Deserialize)]
/// enum Status {
///     Pending,
///     Running { pid: u32 },
///     Failed { error: String },
/// }
/// ```
///
/// This generates `DatabaseConfigTarget` with `Serialize`, `Deserialize`, `Debug`, and `Clone` derives,
/// and implements `State` for `DatabaseConfig`. For the enum `Status`, the target type is `Status` itself (Target = Self).
#[proc_macro_derive(State, attributes(mahler))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_model_derive(input).unwrap_or_else(|err| err.to_compile_error().into())
}
