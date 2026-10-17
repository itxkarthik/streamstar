{
  description = "Streamstar: drift-adaptive federated streaming analytics in Go";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  # Track the newest stable Go without upgrading the rest of the environment.
  inputs.nixpkgs-go.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  outputs = { self, nixpkgs, nixpkgs-go }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      goFor = system: (import nixpkgs-go { inherit system; }).go_latest;
    in {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          # Built from ui/ rather than trusting the committed copy in internal/server/web.
          ui = pkgs.buildNpmPackage {
            pname = "streamstar-ui";
            version = "0.1.0";
            src = pkgs.lib.cleanSourceWith {
              src = ./ui;
              filter = path: type: !(builtins.elem (baseNameOf path) [ "node_modules" ".astro" ]);
            };
            npmDepsHash = "sha256-Gl82ElRvVA0XrnTdP5fKIorG3IB5ckTS9WVDOaM2MqM=";
            env = { ASTRO_TELEMETRY_DISABLED = "1"; STREAMSTAR_UI_OUT = "dist"; };
            installPhase = "cp -r dist $out";
          };
        in {
          inherit ui;
          default = (pkgs.buildGoModule.override { go = goFor system; }) {
            pname = "streamstar";
            version = "0.1.0";
            src = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter = path: type:
                let name = baseNameOf path;
                in !(builtins.elem name [ ".git" ".firecrawl" ".direnv" "bin" "data" "results" "result" "node_modules" ]);
            };
            vendorHash = "sha256-41Oj/HbQlKawCa3K/1D4tkm91STRkPW32UXUkNfQe+4=";
            subPackages = [ "cmd/streamstar" ];
            preBuild = ''
              rm -rf internal/server/web
              cp -r ${ui} internal/server/web
            '';
          };
        });
      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/streamstar";
        };
      });
      checks = forAllSystems (system: {
        build-and-tests = self.packages.${system}.default;
      });
      devShells = forAllSystems (system:
        let pkgs = import nixpkgs { inherit system; };
        in rec {
          default = pkgs.mkShell {
            packages = with pkgs; [ (goFor system) gopls gotools go-tools git gh gnumake curl jq python3 nodejs_22 direnv nix-direnv (docker_29.override { clientOnly = true; }) docker-compose ];
            shellHook = ''
              export STREAMSTAR_DEV=1
              echo "Streamstar · make run | make test | make experiment | make ui"
            '';
          };
          kafka = pkgs.mkShell {
            inputsFrom = [ default ];
            packages = [ pkgs.apacheKafka ];
          };
        });
    };
}
