// Automatically executed by tonic-build + tonic-prost-build
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/plugin.proto")?;
    Ok(())
}
