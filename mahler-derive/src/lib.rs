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
//!
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

fn extract_mahler_derives(attrs: &[Attribute]) -> Result<Option<proc_macro2::TokenStream>> {
    for attr in attrs {
        if attr.path().is_ident("mahler") {
            if let Meta::List(meta_list) = &attr.meta {
                let tokens_str = meta_list.tokens.to_string();

                // Check if it starts with "derive"
                if let Some(derives_part) = tokens_str.strip_prefix("derive") {
                    // Parse the derives - expect format: (Trait1, Trait2, ...)
                    let derives_part = derives_part.trim();
                    if derives_part.starts_with('(') && derives_part.ends_with(')') {
                        let inner = &derives_part[1..derives_part.len() - 1];

                        // Parse the comma-separated list of trait names
                        let traits: Vec<_> = inner
                            .split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .collect();

                        if !traits.is_empty() {
                            // Build the token stream for derives
                            let trait_tokens: Vec<proc_macro2::TokenStream> = traits
                                .iter()
                                .map(|t| {
                                    let ident = syn::Ident::new(t, proc_macro2::Span::call_site());
                                    quote! { #ident }
                                })
                                .collect();

                            return Ok(Some(quote! { #(#trait_tokens),* }));
                        }
                    }
                }
            }
        }
    }
    Ok(None)
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

fn expand_state_derive(input: DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    let target_name = format_ident!("{}Target", struct_name);
    let visibility = &input.vis;

    // Extract generics information
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Use `mahler::state` path since that's the published crate
    let model_path = quote! { ::mahler::state::State };

    // Check for derives on the parent struct
    let extra_derives = extract_mahler_derives(&input.attrs)?;
    let has_extra_derives = extra_derives.is_some();

    // Configure sensible default derives for the target struct
    let derives = if let Some(extra) = extra_derives {
        quote! { #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone, #extra)] }
    } else {
        quote! { #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)] }
    };

    match input.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields) => {
                    // Handle named fields (struct { field: Type })
                    let target_fields = process_named_fields(&fields.named)?;

                    // Preserve all attributes from the original struct except mahler specific ones
                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        #derives
                        #(#struct_attrs)*
                        #visibility struct #target_name #generics #where_clause {
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

                    // Preserve all attributes from the original struct
                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        #derives
                        #(#struct_attrs)*
                        #visibility struct #target_name #generics (#(#target_fields,)*) #where_clause;

                        // Implement the State trait
                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;
                        }
                    };

                    Ok(expanded.into())
                }
                Fields::Unit => {
                    // Unit structs never have internal fields
                    // Error if #[mahler(derive(...))] is present
                    if has_extra_derives {
                        return Err(Error::new_spanned(
                            struct_name,
                            "#[mahler(derive(...))] cannot be used on enums. Add the derives directly to the parent type instead."
                        ));
                    }

                    // Create type alias and use Target = Self
                    let expanded = quote! {
                        // Create a type alias for consistency
                        #visibility type #target_name #generics = #struct_name #ty_generics #where_clause;

                        // Implement the State trait
                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = Self;
                        }
                    };

                    Ok(expanded.into())
                }
            }
        }
        Data::Enum(_) => {
            // For enums, Target = Self since enums don't have internal fields
            // Error if #[mahler(derive(...))] is present
            if has_extra_derives {
                return Err(Error::new_spanned(
                    struct_name,
                    "#[mahler(derive(...))] cannot be used on enums. Add the derives directly to the parent type instead."
                ));
            }

            let expanded = quote! {
                // Create a type alias for consistency
                #visibility type #target_name #generics = #struct_name #ty_generics #where_clause;

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
/// - `#[mahler(derive(Trait1, Trait2, ...))]` - Adds additional derives to the generated target struct.
///   Only applies when a new target struct is created (i.e. when the source structure is not an
///   enum or a unit type).
///   The derives are added in addition to the default `Serialize`, `Deserialize`, `Debug`, and `Clone` derives.
///
/// # Examples
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
/// // This generates `DatabaseConfigTarget` with `Serialize`, `Deserialize`, `Debug`, and `Clone` derives,
/// // and implements `State` for `DatabaseConfig`.
///
/// #[derive(State, Serialize, Deserialize)]
/// enum Status {
///     Pending,
///     Running { pid: u32 },
///     Failed { error: String },
/// }
/// // Generates: pub type StatusTarget = Status;
/// // And: impl State for Status { type Target = Self; }
///
/// // Struct with additional derives on target
/// #[derive(State, Serialize, Deserialize)]
/// #[mahler(derive(PartialEq, Eq))]
/// struct Database {
///     host: String,
///     port: u16,
///     #[mahler(internal)]
///     connection_pool: Option<String>,
/// }
/// // Generates: #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
/// //            pub struct DatabaseTarget { host: String, port: u16 }
/// ```
#[proc_macro_derive(State, attributes(mahler))]
pub fn derive_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_state_derive(input).unwrap_or_else(|err| err.to_compile_error().into())
}
