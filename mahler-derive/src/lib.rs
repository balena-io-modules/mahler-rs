//! Derive macros for the mahler automated job orchestration library.
//!
//! This crate provides procedural macros to simplify working with mahler models and state management.
//!
//! # State Derive Macro
//!
//! The `#[derive(State)]` macro automatically implements the
//! [State](`State`) trait, generates a Target type, and provides
//! `serde::Serialize` and `serde::Deserialize` implementations for both types.
//!
//! ```rust
//! use mahler::state::State;
//!
//! #[derive(State, Debug, Clone)]
//! struct Service {
//!     name: String,
//!
//!     image: Option<String>,
//!
//!     // This field won't appear in ServiceTarget
//!     #[mahler(internal)]
//!     container_id: Option<String>,
//! }
//! ```
//!
//! This generates:
//! - Implementations of `Serialize` and `Deserialize` for `Service`
//! - A `ServiceTarget` struct with the same fields except those marked `#[mahler(internal)]`
//! - Implementations of `Serialize` and `Deserialize` for `ServiceTarget`
//! - An implementation of `State` for `Service`

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Error, Field, Fields, Meta, Result};

type NamedTargetFields = Vec<proc_macro2::TokenStream>;
type UnnamedTargetFields = Vec<proc_macro2::TokenStream>;

// ============================================================================
// Parsing functions
// ============================================================================

/// Parse whether a field has the #[mahler(internal)] attribute
fn has_mahler_internal_attribute(attrs: &[Attribute]) -> Result<bool> {
    for attr in attrs {
        if attr.path().is_ident("mahler") {
            match &attr.meta {
                Meta::List(meta_list) => {
                    let tokens = &meta_list.tokens;
                    if tokens.to_string().trim() == "internal" {
                        return Ok(true);
                    }
                }
                Meta::Path(_) => {}
                Meta::NameValue(_) => {}
            }
        }
    }
    Ok(false)
}

/// Extract additional derives from #[mahler(derive(...))] attribute
fn extract_mahler_derives(attrs: &[Attribute]) -> Result<Option<proc_macro2::TokenStream>> {
    for attr in attrs {
        if attr.path().is_ident("mahler") {
            if let Meta::List(meta_list) = &attr.meta {
                let tokens_str = meta_list.tokens.to_string();

                if let Some(derives_part) = tokens_str.strip_prefix("derive") {
                    let derives_part = derives_part.trim();
                    if derives_part.starts_with('(') && derives_part.ends_with(')') {
                        let inner = &derives_part[1..derives_part.len() - 1];

                        let traits: Vec<_> = inner
                            .split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .collect();

                        if !traits.is_empty() {
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

/// Extract should_halt_if function name from #[mahler(should_halt_if = "fn")] attribute
fn extract_should_halt_if(attrs: &[Attribute]) -> Result<Option<String>> {
    for attr in attrs {
        if attr.path().is_ident("mahler") {
            if let Meta::List(meta_list) = &attr.meta {
                let tokens_str = meta_list.tokens.to_string();

                if let Some(function_part) = tokens_str.strip_prefix("should_halt_if") {
                    let function_part = function_part.trim();
                    if let Some(function_part) = function_part.strip_prefix('=') {
                        let function_part = function_part.trim();
                        let function_name = function_part.trim_matches('"').trim();
                        if !function_name.is_empty() {
                            return Ok(Some(function_name.to_string()));
                        }
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Filter attributes, removing mahler and derive attributes
fn filter_attributes(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("mahler") && !attr.path().is_ident("derive"))
        .collect()
}

/// Filter field attributes, removing mahler attributes
fn filter_field_attributes(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("mahler"))
        .collect()
}

/// Validate that an enum has no internal values and return error if it does
fn validate_enum_variants(data_enum: &syn::DataEnum) -> Result<()> {
    for variant in &data_enum.variants {
        match &variant.fields {
            Fields::Unit => {}
            Fields::Named(_) | Fields::Unnamed(_) => {
                return Err(Error::new_spanned(
                    variant,
                    "State can only be derived for enums with unit variants (no fields). Enums with data are not supported.",
                ));
            }
        }
    }
    Ok(())
}

/// Process named fields to generate target field definitions
fn process_named_fields(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
) -> Result<NamedTargetFields> {
    let mut target_fields = NamedTargetFields::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_vis = &field.vis;

        let is_internal = has_mahler_internal_attribute(&field.attrs)?;

        if !is_internal {
            let target_attrs = filter_field_attributes(&field.attrs);

            target_fields.push(quote! {
                #(#target_attrs)*
                #field_vis #field_name: <#field_type as ::mahler::state::State>::Target
            });
        }
    }

    Ok(target_fields)
}

/// Process unnamed fields to generate target field definitions
fn process_unnamed_fields(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
) -> Result<UnnamedTargetFields> {
    let mut target_fields = UnnamedTargetFields::new();

    for field in fields.iter() {
        let field_type = &field.ty;

        let is_internal = has_mahler_internal_attribute(&field.attrs)?;

        if is_internal {
            return Err(Error::new_spanned(
                field,
                "#[mahler(internal)] is only supported on named struct fields, not tuple struct fields",
            ));
        }

        let target_attrs = filter_field_attributes(&field.attrs);

        target_fields.push(quote! {
            #(#target_attrs)*
            <#field_type as ::mahler::state::State>::Target
        });
    }

    Ok(target_fields)
}

/// Check if a type is Option<T>
fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Check if a type is a collection type (List, Set, or Map)
fn is_collection_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            let ident = &segment.ident;
            return ident == "List" || ident == "Set" || ident == "Map";
        }
    }
    false
}

// ============================================================================
// Code generation functions
// ============================================================================

/// Generate Serialize implementation for a struct with named fields
fn generate_serialize_impl(
    struct_name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    generics: &syn::Generics,
    exclude_internal: bool,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let all_fields: Vec<_> = fields.iter().collect();

    let relevant_fields: Vec<_> = if exclude_internal {
        all_fields
            .iter()
            .filter(|f| !has_mahler_internal_attribute(&f.attrs).unwrap_or(false))
            .copied()
            .collect()
    } else {
        all_fields
    };

    let field_count = relevant_fields.len();
    let struct_name_str = struct_name.to_string();

    let field_serializations: Vec<_> = relevant_fields
        .iter()
        .filter_map(|field| {
            let field_name = field.ident.as_ref()?;
            let field_name_str = field_name.to_string();
            let is_option = is_option_type(&field.ty);

            if is_option {
                // Skip None values for Option fields
                Some(quote! {
                    if self.#field_name.is_some() {
                        __state.serialize_field(#field_name_str, &self.#field_name)?;
                    }
                })
            } else {
                Some(quote! {
                    __state.serialize_field(#field_name_str, &self.#field_name)?;
                })
            }
        })
        .collect();

    quote! {
        impl #impl_generics ::mahler::serde::Serialize for #struct_name #ty_generics #where_clause {
            fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
            where
                __S: ::mahler::serde::Serializer,
            {
                use ::mahler::serde::ser::SerializeStruct;
                let mut __state = __serializer.serialize_struct(#struct_name_str, #field_count)?;
                #(#field_serializations)*
                __state.end()
            }
        }
    }
}

/// Generate Deserialize implementation for a struct with named fields
fn generate_deserialize_impl(
    struct_name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    generics: &syn::Generics,
    exclude_internal: bool,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let all_fields: Vec<_> = fields.iter().collect();

    let relevant_fields: Vec<_> = if exclude_internal {
        all_fields
            .iter()
            .filter(|f| !has_mahler_internal_attribute(&f.attrs).unwrap_or(false))
            .copied()
            .collect()
    } else {
        all_fields
    };

    let field_names: Vec<_> = relevant_fields
        .iter()
        .filter_map(|f| f.ident.as_ref())
        .collect();
    let field_strs: Vec<_> = field_names.iter().map(|n| n.to_string()).collect();
    let struct_name_str = struct_name.to_string();

    // Generate field assignments based on whether they're Option or collection types
    let field_assignments: Vec<_> = relevant_fields
        .iter()
        .filter_map(|field| {
            let field_name = field.ident.as_ref()?;
            let field_str = field_name.to_string();

            if is_option_type(&field.ty) || is_collection_type(&field.ty) {
                Some(quote! {
                    #field_name: #field_name.unwrap_or_default()
                })
            } else {
                Some(quote! {
                    #field_name: #field_name.ok_or_else(|| ::mahler::serde::de::Error::missing_field(#field_str))?
                })
            }
        })
        .collect();

    // Handle generics with the de lifetime
    let de_impl_generics = if generics.params.is_empty() {
        quote! { <'de> }
    } else {
        let params = &generics.params;
        quote! { <'de, #params> }
    };

    let de_where_clause = if let Some(wc) = where_clause {
        quote! { #wc }
    } else {
        quote! {}
    };

    quote! {
        impl #de_impl_generics ::mahler::serde::Deserialize<'de> for #struct_name #ty_generics #de_where_clause {
            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
            where
                __D: ::mahler::serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                enum __Field {
                    #(#field_names,)*
                    __ignore,
                }

                impl<'de> ::mahler::serde::Deserialize<'de> for __Field {
                    fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                    where
                        __D: ::mahler::serde::Deserializer<'de>,
                    {
                        struct __FieldVisitor;

                        impl<'de> ::mahler::serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;

                            fn expecting(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                __formatter.write_str("field identifier")
                            }

                            fn visit_str<__E>(self, __value: &str) -> ::core::result::Result<Self::Value, __E>
                            where
                                __E: ::mahler::serde::de::Error,
                            {
                                match __value {
                                    #(#field_strs => Ok(__Field::#field_names),)*
                                    _ => Ok(__Field::__ignore),
                                }
                            }
                        }

                        __deserializer.deserialize_identifier(__FieldVisitor)
                    }
                }

                struct __Visitor #impl_generics #where_clause {
                    __phantom: ::core::marker::PhantomData<#struct_name #ty_generics>,
                }

                impl #de_impl_generics ::mahler::serde::de::Visitor<'de> for __Visitor #ty_generics #de_where_clause {
                    type Value = #struct_name #ty_generics;

                    fn expecting(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                        __formatter.write_str(concat!("struct ", #struct_name_str))
                    }

                    fn visit_map<__A>(self, mut __map: __A) -> ::core::result::Result<Self::Value, __A::Error>
                    where
                        __A: ::mahler::serde::de::MapAccess<'de>,
                    {
                        #(let mut #field_names: Option<_> = None;)*

                        while let Some(__key) = __map.next_key()? {
                            match __key {
                                #(__Field::#field_names => {
                                    if #field_names.is_some() {
                                        return Err(::mahler::serde::de::Error::duplicate_field(#field_strs));
                                    }
                                    #field_names = Some(__map.next_value()?);
                                })*
                                __Field::__ignore => {
                                    let _ = __map.next_value::<::mahler::serde::de::IgnoredAny>()?;
                                }
                            }
                        }

                        Ok(#struct_name {
                            #(#field_assignments,)*
                        })
                    }
                }

                __deserializer.deserialize_struct(
                    #struct_name_str,
                    &[#(#field_strs,)*],
                    __Visitor { __phantom: ::core::marker::PhantomData }
                )
            }
        }
    }
}

/// Generate the as_internal method for structs with named fields
fn generate_as_internal_impl(
    struct_name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
) -> proc_macro2::TokenStream {
    let all_fields: Vec<_> = fields.iter().collect();
    let field_count = all_fields.len() + 1; // +1 for halted
    let struct_name_str = struct_name.to_string();

    let field_serializations: Vec<_> = all_fields
        .iter()
        .filter_map(|field| {
            let field_name = field.ident.as_ref()?;
            let field_name_str = field_name.to_string();

            let is_internal = has_mahler_internal_attribute(&field.attrs).unwrap_or(false);
            let is_option = is_option_type(&field.ty);

            if is_internal {
                // Internal fields serialize directly without AsInternal wrapper
                if is_option {
                    // Skip None values for internal Option fields
                    Some(quote! {
                        if self.#field_name.is_some() {
                            __state.serialize_field(
                                #field_name_str,
                                &self.#field_name
                            )?;
                        }
                    })
                } else {
                    Some(quote! {
                        __state.serialize_field(
                            #field_name_str,
                            &self.#field_name
                        )?;
                    })
                }
            } else {
                // Non-internal fields use AsInternal wrapper
                if is_option {
                    // Skip None values for non-internal Option fields
                    Some(quote! {
                        if self.#field_name.is_some() {
                            __state.serialize_field(
                                #field_name_str,
                                &::mahler::state::AsInternal(&self.#field_name)
                            )?;
                        }
                    })
                } else {
                    Some(quote! {
                        __state.serialize_field(
                            #field_name_str,
                            &::mahler::state::AsInternal(&self.#field_name)
                        )?;
                    })
                }
            }
        })
        .collect();

    quote! {
        fn as_internal<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
        where
            __S: ::mahler::serde::Serializer,
        {
            use ::mahler::serde::ser::SerializeStruct;

            let mut __state = __serializer.serialize_struct(#struct_name_str, #field_count)?;

            __state.serialize_field("__mahler(halted)", &self.is_halted())?;

            #(#field_serializations)*

            __state.end()
        }
    }
}

/// Generate is_halted implementation if should_halt_if is specified
fn generate_is_halted_impl(should_halt_if_fn: &Option<String>) -> proc_macro2::TokenStream {
    if let Some(ref fn_name) = should_halt_if_fn {
        let fn_path: syn::Path =
            syn::parse_str(fn_name).expect("Failed to parse should_halt_if function path");
        quote! {
            fn is_halted(&self) -> bool {
                #fn_path(self)
            }
        }
    } else {
        quote! {}
    }
}

/// Generate derive attributes for the target type
fn generate_target_derives(
    extra_derives: Option<proc_macro2::TokenStream>,
) -> proc_macro2::TokenStream {
    if let Some(extra) = extra_derives {
        quote! { #[derive(Debug, Clone, #extra)] }
    } else {
        quote! { #[derive(Debug, Clone)] }
    }
}

// ============================================================================
// Main expansion function
// ============================================================================

fn expand_state_derive(input: DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    let target_name = format_ident!("{}Target", struct_name);
    let visibility = &input.vis;

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let model_path = quote! { ::mahler::state::State };

    let extra_derives = extract_mahler_derives(&input.attrs)?;
    let has_extra_derives = extra_derives.is_some();
    let should_halt_if_fn = extract_should_halt_if(&input.attrs)?;

    let derives = generate_target_derives(extra_derives);

    match input.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields) => {
                    let target_fields = process_named_fields(&fields.named)?;
                    let as_internal_impl = generate_as_internal_impl(struct_name, &fields.named);
                    let is_halted_impl = generate_is_halted_impl(&should_halt_if_fn);
                    let serialize_impl =
                        generate_serialize_impl(struct_name, &fields.named, generics, false);
                    let deserialize_impl =
                        generate_deserialize_impl(struct_name, &fields.named, generics, false);
                    let target_serialize_impl =
                        generate_serialize_impl(&target_name, &fields.named, generics, true);
                    let target_deserialize_impl =
                        generate_deserialize_impl(&target_name, &fields.named, generics, true);

                    let struct_attrs = filter_attributes(&input.attrs);

                    let expanded = quote! {
                        #serialize_impl
                        #deserialize_impl

                        #derives
                        #(#struct_attrs)*
                        #visibility struct #target_name #generics #where_clause {
                            #(#target_fields,)*
                        }

                        #target_serialize_impl
                        #target_deserialize_impl

                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;

                            #is_halted_impl

                            #as_internal_impl
                        }
                    };

                    Ok(expanded.into())
                }
                Fields::Unnamed(fields) => {
                    // Only allow single-field tuple structs (newtypes)
                    if fields.unnamed.len() != 1 {
                        return Err(Error::new_spanned(
                            struct_name,
                            "State can only be derived for tuple structs with exactly one field (newtypes). Multi-field tuple structs are not supported.",
                        ));
                    }

                    let target_fields = process_unnamed_fields(&fields.unnamed)?;
                    let halted_impl = generate_is_halted_impl(&should_halt_if_fn);

                    let struct_attrs = filter_attributes(&input.attrs);

                    let field_type = &fields.unnamed.iter().next().unwrap().ty;

                    let de_impl_generics = if generics.params.is_empty() {
                        quote! { <'de> }
                    } else {
                        let params = &generics.params;
                        quote! { <'de, #params> }
                    };

                    let de_where_clause = if let Some(wc) = where_clause {
                        quote! { #wc }
                    } else {
                        quote! {}
                    };

                    let expanded = quote! {
                        impl #impl_generics ::mahler::serde::Serialize for #struct_name #ty_generics #where_clause {
                            fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
                            where
                                __S: ::mahler::serde::Serializer,
                            {
                                self.0.serialize(__serializer)
                            }
                        }

                        impl #de_impl_generics ::mahler::serde::Deserialize<'de> for #struct_name #ty_generics #de_where_clause {
                            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                            where
                                __D: ::mahler::serde::Deserializer<'de>,
                            {
                                Ok(#struct_name(<#field_type as ::mahler::serde::Deserialize>::deserialize(__deserializer)?))
                            }
                        }

                        #derives
                        #(#struct_attrs)*
                        #visibility struct #target_name #generics (#(#target_fields,)*) #where_clause;

                        impl #impl_generics ::mahler::serde::Serialize for #target_name #ty_generics #where_clause {
                            fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
                            where
                                __S: ::mahler::serde::Serializer,
                            {
                                self.0.serialize(__serializer)
                            }
                        }

                        impl #de_impl_generics ::mahler::serde::Deserialize<'de> for #target_name #ty_generics #de_where_clause {
                            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                            where
                                __D: ::mahler::serde::Deserializer<'de>,
                            {
                                Ok(#target_name(<#field_type as ::mahler::serde::Deserialize>::deserialize(__deserializer)?))
                            }
                        }

                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = #target_name #ty_generics;

                            #halted_impl
                        }
                    };

                    Ok(expanded.into())
                }
                Fields::Unit => {
                    if has_extra_derives {
                        return Err(Error::new_spanned(
                            struct_name,
                            "#[mahler(derive(...))] cannot be used on unit structs. Add the derives directly to the parent type instead.",
                        ));
                    }

                    let halted_impl = generate_is_halted_impl(&should_halt_if_fn);
                    let struct_name_str = struct_name.to_string();

                    let de_impl_generics = if generics.params.is_empty() {
                        quote! { <'de> }
                    } else {
                        let params = &generics.params;
                        quote! { <'de, #params> }
                    };

                    let de_where_clause = if let Some(wc) = where_clause {
                        quote! { #wc }
                    } else {
                        quote! {}
                    };

                    let expanded = quote! {
                        impl #impl_generics ::mahler::serde::Serialize for #struct_name #ty_generics #where_clause {
                            fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
                            where
                                __S: ::mahler::serde::Serializer,
                            {
                                __serializer.serialize_unit_struct(#struct_name_str)
                            }
                        }

                        impl #de_impl_generics ::mahler::serde::Deserialize<'de> for #struct_name #ty_generics #de_where_clause {
                            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                            where
                                __D: ::mahler::serde::Deserializer<'de>,
                            {
                                struct __Visitor;

                                impl<'de> ::mahler::serde::de::Visitor<'de> for __Visitor {
                                    type Value = #struct_name;

                                    fn expecting(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                        __formatter.write_str(concat!("unit struct ", #struct_name_str))
                                    }

                                    fn visit_unit<__E>(self) -> ::core::result::Result<Self::Value, __E>
                                    where
                                        __E: ::mahler::serde::de::Error,
                                    {
                                        Ok(#struct_name)
                                    }
                                }

                                __deserializer.deserialize_unit_struct(#struct_name_str, __Visitor)
                            }
                        }

                        #visibility type #target_name #generics = #struct_name #ty_generics #where_clause;

                        impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                            type Target = Self;

                            #halted_impl
                        }
                    };

                    Ok(expanded.into())
                }
            }
        }
        Data::Enum(data_enum) => {
            validate_enum_variants(&data_enum)?;

            if has_extra_derives {
                return Err(Error::new_spanned(
                    struct_name,
                    "#[mahler(derive(...))] cannot be used on enums. Add the derives directly to the parent type instead.",
                ));
            }

            let halted_impl = generate_is_halted_impl(&should_halt_if_fn);
            let struct_name_str = struct_name.to_string();

            let variants: Vec<_> = data_enum.variants.iter().collect();
            let variant_names: Vec<_> = variants.iter().map(|v| &v.ident).collect();
            let variant_strs: Vec<_> = variant_names.iter().map(|n| n.to_string()).collect();

            let de_impl_generics = if generics.params.is_empty() {
                quote! { <'de> }
            } else {
                let params = &generics.params;
                quote! { <'de, #params> }
            };

            let de_where_clause = if let Some(wc) = where_clause {
                quote! { #wc }
            } else {
                quote! {}
            };

            let expanded = quote! {
                impl #impl_generics ::mahler::serde::Serialize for #struct_name #ty_generics #where_clause {
                    fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
                    where
                        __S: ::mahler::serde::Serializer,
                    {
                        match self {
                            #(#struct_name::#variant_names => __serializer.serialize_unit_variant(
                                #struct_name_str,
                                0,
                                #variant_strs
                            ),)*
                        }
                    }
                }

                impl #de_impl_generics ::mahler::serde::Deserialize<'de> for #struct_name #ty_generics #de_where_clause {
                    fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                    where
                        __D: ::mahler::serde::Deserializer<'de>,
                    {
                        #[allow(non_camel_case_types)]
                        enum __Field {
                            #(#variant_names,)*
                        }

                        impl<'de> ::mahler::serde::Deserialize<'de> for __Field {
                            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
                            where
                                __D: ::mahler::serde::Deserializer<'de>,
                            {
                                struct __FieldVisitor;

                                impl<'de> ::mahler::serde::de::Visitor<'de> for __FieldVisitor {
                                    type Value = __Field;

                                    fn expecting(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                        __formatter.write_str("variant identifier")
                                    }

                                    fn visit_u64<__E>(self, __value: u64) -> ::core::result::Result<Self::Value, __E>
                                    where
                                        __E: ::mahler::serde::de::Error,
                                    {
                                        Err(::mahler::serde::de::Error::invalid_value(
                                            ::mahler::serde::de::Unexpected::Unsigned(__value),
                                            &"string"
                                        ))
                                    }

                                    fn visit_str<__E>(self, __value: &str) -> ::core::result::Result<Self::Value, __E>
                                    where
                                        __E: ::mahler::serde::de::Error,
                                    {
                                        match __value {
                                            #(#variant_strs => Ok(__Field::#variant_names),)*
                                            _ => Err(::mahler::serde::de::Error::unknown_variant(__value, &[#(#variant_strs,)*])),
                                        }
                                    }

                                    fn visit_bytes<__E>(self, __value: &[u8]) -> ::core::result::Result<Self::Value, __E>
                                    where
                                        __E: ::mahler::serde::de::Error,
                                    {
                                        match __value {
                                            #(v if v == #variant_strs.as_bytes() => Ok(__Field::#variant_names),)*
                                            _ => {
                                                let __value = &::core::str::from_utf8(__value).unwrap_or("\u{fffd}");
                                                Err(::mahler::serde::de::Error::unknown_variant(__value, &[#(#variant_strs,)*]))
                                            }
                                        }
                                    }
                                }

                                __deserializer.deserialize_identifier(__FieldVisitor)
                            }
                        }

                        struct __Visitor #impl_generics #where_clause {
                            __phantom: ::core::marker::PhantomData<#struct_name #ty_generics>,
                        }

                        impl #de_impl_generics ::mahler::serde::de::Visitor<'de> for __Visitor #ty_generics #de_where_clause {
                            type Value = #struct_name #ty_generics;

                            fn expecting(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                __formatter.write_str(concat!("enum ", #struct_name_str))
                            }

                            fn visit_enum<__A>(self, __data: __A) -> ::core::result::Result<Self::Value, __A::Error>
                            where
                                __A: ::mahler::serde::de::EnumAccess<'de>,
                            {
                                use ::mahler::serde::de::VariantAccess;
                                match __data.variant()? {
                                    #((__Field::#variant_names, __variant) => {
                                        __variant.unit_variant()?;
                                        Ok(#struct_name::#variant_names)
                                    })*
                                }
                            }
                        }

                        __deserializer.deserialize_enum(
                            #struct_name_str,
                            &[#(#variant_strs,)*],
                            __Visitor { __phantom: ::core::marker::PhantomData }
                        )
                    }
                }

                #visibility type #target_name #generics = #struct_name #ty_generics #where_clause;

                impl #impl_generics #model_path for #struct_name #ty_generics #where_clause {
                    type Target = Self;

                    #halted_impl
                }
            };

            Ok(expanded.into())
        }
        _ => Err(Error::new_spanned(
            struct_name,
            "State can only be derived for structs, simple enums and newtypes",
        )),
    }
}

/// Derives the `State` trait for a struct or enum, generating a compatible target type.
///
/// This macro automatically provides `serde::Serialize` and `serde::Deserialize` implementations for both
/// the source type and generated target type. The implementations are compatible, meaning
/// the target type can deserialize what the source type serializes (excluding internal fields).
///
/// For structs, this macro creates a `{StructName}Target` type that excludes fields marked
/// with `#[mahler(internal)]`. For enums (unit variants only), the target type is the same
/// as the original enum.
///
/// # Restrictions
///
/// - **Structs**: Named field structs are fully supported. Tuple structs must have exactly
///   one field (newtype pattern).
/// - **Enums**: Only enums with unit variants (no fields) are supported. Enums with data
///   in variants are not allowed.
///
/// # Attributes
///
/// - `#[mahler(internal)]` - Marks a struct field as internal-only, excluding it from the target type.
///   Only supported on named struct fields, not tuple struct, unit struct, or enum fields.
/// - `#[mahler(derive(Trait1, Trait2, ...))]` - Adds additional derives to the generated target struct.
///   Only applies when a new target struct is created (i.e. when the source structure is not an
///   enum or a unit type).
///   The derives are added in addition to the default `Debug` and `Clone` derives.
/// - `#[mahler(should_halt_if = "function_name")]` - Specifies a function to determine if the state is halted.
///   The function must have the signature `fn(&T) -> bool` where `T` is the type being derived.
///   When provided, generates a `is_halted()` implementation that calls the specified function.
///   A halted state doesn't admit changes during planning.
///
/// # Examples
///
/// ```rust
/// use mahler::state::State;
///
/// #[derive(State)]
/// struct DatabaseConfig {
///     host: String,
///     port: u16,
///
///     database: Option<String>,
///
///     // Internal field - not part of target
///     #[mahler(internal)]
///     connection_pool: Option<String>,
/// }
/// // This generates `DatabaseConfigTarget` with `Serialize`, `Deserialize`, `Debug`, and `Clone`,
/// // and implements `State` for `DatabaseConfig`.
/// // Both `DatabaseConfig` and `DatabaseConfigTarget` have compatible Serialize + Deserialize.
///
/// #[derive(State)]
/// enum Status {
///     Pending,
///     Running,
///     Failed,
/// }
/// // Generates: pub type StatusTarget = Status;
/// // And: impl State for Status { type Target = Self; }
///
/// // Struct with additional derives on target
/// #[derive(State)]
/// #[mahler(derive(PartialEq, Eq))]
/// struct Database {
///     host: String,
///     port: u16,
///     #[mahler(internal)]
///     connection_pool: Option<String>,
/// }
/// // Generates: #[derive(Debug, Clone, PartialEq, Eq)]
/// //            pub struct DatabaseTarget { host: String, port: u16 }
///
/// // Newtype pattern is allowed
/// #[derive(State)]
/// struct UserId(u64);
///
/// // Multi-field tuple structs are NOT allowed
/// // #[derive(State)]
/// // struct Point(f64, f64); // ERROR: only single-field tuple structs allowed
///
/// // Enums with data are NOT allowed
/// // #[derive(State)]
/// // enum Status {
/// //     Running { pid: u32 }, // ERROR: enum variants cannot have fields
/// // }
/// ```
#[proc_macro_derive(State, attributes(mahler))]
pub fn derive_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_state_derive(input).unwrap_or_else(|err| err.to_compile_error().into())
}
