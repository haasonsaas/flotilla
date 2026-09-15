{
  description = "flotilla: leaderless fleet coordination over Tailscale";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = (pkgs.lib.importTOML ./Cargo.toml).workspace.package;
      in
      {
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "flotilla";
          version = manifest.version;
          src = pkgs.lib.cleanSource ./.;
          cargoLock.lockFile = ./Cargo.lock;
          # The daemon shells out to the tailscale CLI at runtime; it is
          # located on PATH or at the usual install locations, not linked.
          doCheck = false;
          meta = with pkgs.lib; {
            description = "Leaderless fleet coordination for Macs and Linux boxes over Tailscale";
            homepage = "https://github.com/haasonsaas/flotilla";
            license = licenses.mit;
            mainProgram = "flotilla";
          };
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/flotilla";
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [ cargo rustc rustfmt clippy rust-analyzer ];
        };
      });
}
