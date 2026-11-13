use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc_path);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../proto/aegis/v1/sandbox.proto"], &["../proto"])?;

    println!("cargo:rerun-if-changed=../proto/aegis/v1/sandbox.proto");
    Ok(())
}
