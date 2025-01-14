# Changelog

All notable changes to iroh-blobs will be documented in this file.

## [0.31.0](https://github.com/n0-computer/iroh-blobs/compare/v0.30.0..0.31.0) - 2025-01-14

### ⚙️ Miscellaneous Tasks

- *(deps)* Bump mozilla-actions/sccache-action ([#40](https://github.com/n0-computer/iroh-blobs/issues/40)) - ([57112e6](https://github.com/n0-computer/iroh-blobs/commit/57112e62618e07a833a261b0dbfd2f64cc22eb82))
- Add project tracking ([#43](https://github.com/n0-computer/iroh-blobs/issues/43)) - ([a279ad1](https://github.com/n0-computer/iroh-blobs/commit/a279ad1bc0472fb4e47df466ab73ed9e0fa0a50a))
- Pin nextest version ([#44](https://github.com/n0-computer/iroh-blobs/issues/44)) - ([b1de3b3](https://github.com/n0-computer/iroh-blobs/commit/b1de3b306135984e113d09531beff9ed6463a778))
- Upgrade to `iroh@v0.31.0` ([#45](https://github.com/n0-computer/iroh-blobs/issues/45)) - ([2b800c9](https://github.com/n0-computer/iroh-blobs/commit/2b800c9264b21dfb73bfecbe9881bc6c07c7e0d1))

## [0.30.0](https://github.com/n0-computer/iroh-blobs/compare/v0.29.0..0.30.0) - 2024-12-17

### ⛰️  Features

- Update to new protocolhandler ([#29](https://github.com/n0-computer/iroh-blobs/issues/29)) - ([dba7850](https://github.com/n0-computer/iroh-blobs/commit/dba7850ae874939bd9a83f97c36dc6eceee7f9bd))
- Import iroh_base::ticket::BlobTicket and iroh_base::hash - ([f9d3ae1](https://github.com/n0-computer/iroh-blobs/commit/f9d3ae1e6a0cbdbdece56b0b3d948f0a3d62118c))
- [**breaking**] Update to iroh@0.30.0 ([#41](https://github.com/n0-computer/iroh-blobs/issues/41)) - ([74f1ee3](https://github.com/n0-computer/iroh-blobs/commit/74f1ee32cca396cd8e4d1cb8815b71e27c98df74))

### 🐛 Bug Fixes

- [**breaking**] Make `net_protocol` feature work without `rpc` ([#27](https://github.com/n0-computer/iroh-blobs/issues/27)) - ([4c1446f](https://github.com/n0-computer/iroh-blobs/commit/4c1446f7578778dadee6db0ad07ff025ef753779))
- Fix the task leak with the lazy in-mem rpc client while still keeping it lazy ([#31](https://github.com/n0-computer/iroh-blobs/issues/31)) - ([9ae2e52](https://github.com/n0-computer/iroh-blobs/commit/9ae2e52431ca6948ba60bca0169bba7d7cde1d06))
- Fix silent failure to add data of more than ~16MB via add_bytes or add_bytes_named ([#36](https://github.com/n0-computer/iroh-blobs/issues/36)) - ([dec9643](https://github.com/n0-computer/iroh-blobs/commit/dec96436772007178a2c9190d87598893a38b57d))

### 🚜 Refactor

- [**breaking**] Make Dialer trait private and inline iroh::dialer::Dialer ([#34](https://github.com/n0-computer/iroh-blobs/issues/34)) - ([d91b2ce](https://github.com/n0-computer/iroh-blobs/commit/d91b2ce7784cfa78fd2e6f9e0fb74f9b950a878f))
- [**breaking**] Remove the migration from redb v1 ([#33](https://github.com/n0-computer/iroh-blobs/issues/33)) - ([ae91f16](https://github.com/n0-computer/iroh-blobs/commit/ae91f16bd466f00e003d064593ea53c4c2276999))
- Simplify quinn rpc test ([#39](https://github.com/n0-computer/iroh-blobs/issues/39)) - ([60dfdbb](https://github.com/n0-computer/iroh-blobs/commit/60dfdbbacdb002621b85378449c66397d4d377f1))

### 📚 Documentation

- Add "getting started" instructions to the readme ([#32](https://github.com/n0-computer/iroh-blobs/issues/32)) - ([dd6673e](https://github.com/n0-computer/iroh-blobs/commit/dd6673e777e8f8ceab462501348941ac2387fab1))

### ⚙️ Miscellaneous Tasks

- Update rcgen to 0.13 ([#35](https://github.com/n0-computer/iroh-blobs/issues/35)) - ([57340cc](https://github.com/n0-computer/iroh-blobs/commit/57340cc931c7e0a3e8e3d14bef00e926ab7cfe47))

## [0.29.0](https://github.com/n0-computer/iroh-blobs/compare/v0.28.1..0.29.0) - 2024-12-04

### ⛰️  Features

- Update to renamed iroh-net ([#18](https://github.com/n0-computer/iroh-blobs/issues/18)) - ([b9dbf2c](https://github.com/n0-computer/iroh-blobs/commit/b9dbf2cc1e6d8a6b60f3bf9f52b832fbd23c394e))
- Update to iroh@0.29.0  - ([e9b136a](https://github.com/n0-computer/iroh-blobs/commit/e9b136ad59d8feddd16df50503bf67206cccedd9))

### 🚜 Refactor

- Avoid `get_protocol`, just keep `Arc<Blobs>` around - ([7b404f1](https://github.com/n0-computer/iroh-blobs/commit/7b404f12bca87b32af85ef0b099cc8174940219f))

### 📚 Documentation

- Add simple file transfer example - ([7ba883d](https://github.com/n0-computer/iroh-blobs/commit/7ba883d238bb2b0be8c64672369c27074519962c))

### 🧪 Testing

- Update iroh-test to 0.29 - ([cda9756](https://github.com/n0-computer/iroh-blobs/commit/cda9756d84b34583f469ffa6f9083b9b3a5fd2a5))

### ⚙️ Miscellaneous Tasks

- Prune some deps ([#12](https://github.com/n0-computer/iroh-blobs/issues/12)) - ([4c7b8e7](https://github.com/n0-computer/iroh-blobs/commit/4c7b8e79f495376245852a14688ce23a12adda85))
- Init changelog - ([acafd9e](https://github.com/n0-computer/iroh-blobs/commit/acafd9ef8fe4851854ac2a48016ebdba215f5b6b))
- Update release config - ([b621f3c](https://github.com/n0-computer/iroh-blobs/commit/b621f3c97416b61d2b7970a5c2b4ab9f5a7d9752))

### Examples

- Import examples from main repo - ([1579555](https://github.com/n0-computer/iroh-blobs/commit/1579555ba3f67102d3e4aafcf7889b558f744460))

## [0.28.1](https://github.com/n0-computer/iroh-blobs/compare/v0.28.0..v0.28.1) - 2024-11-04

### 🐛 Bug Fixes

- Use correctly patched iroh-quinn and iroh-net - ([b3c5f76](https://github.com/n0-computer/iroh-blobs/commit/b3c5f7624716896c085add70215336404188442a))

### ⚙️ Miscellaneous Tasks

- Release iroh-blobs version 0.28.1 - ([191cd2a](https://github.com/n0-computer/iroh-blobs/commit/191cd2a1c25885f8ef0d58d83df150017bc4c8bb))


