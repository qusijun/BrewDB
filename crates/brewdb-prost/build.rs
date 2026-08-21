fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    prost_build::Config::new()
        .compile_protos(&["prost/brewdb/planner/v1/fragment.proto"], &["prost"])?;

    println!("cargo:rerun-if-changed=prost/brewdb/planner/v1/fragment.proto");
    Ok(())
}
