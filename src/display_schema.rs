/*
    helper function to pretty print schema of file
*/
use parquet::schema::types::Type;

fn display_schema(schema: &Type, _depth: usize, col_num: usize) {
    let name = schema.name();
    let indent = " ".repeat(4 * _depth);
    // RUST 101 - the dot dot means we don't care about the other fields in schema, otherwise we'd have to list them all
    // match <thing to match>
    //     <specific match for type or value> { <field of thing>, <field2 of thing> ... } => do stuff
    //     <other match for type or value> etc etc ...
    match schema {
        Type::PrimitiveType { physical_type, .. } => println!("{}{}) {} : {}", indent, col_num, name, physical_type),
        Type::GroupType { .. } => println!("{}{} is a list type", indent, name),
    }

    // for nested schema, where is list of other types
    if schema.is_group() {
        println!("Found group for schema {}", name) ;
        let mut column_num = 0 ;
        for column in schema.get_fields() {
            display_schema(column, _depth + 1, column_num);
            column_num += 1;
        }
    }
}
