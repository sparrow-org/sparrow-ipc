from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build.cppstd import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy
from conan.tools.scm import Version
import os

required_conan_version = ">=2.0"

class SparrowIPCRecipe(ConanFile):
    name = "sparrow-ipc"
    description = "C++20 library for memory-mapped serialization of Apache Arrow data."
    license = "BSD-3-Clause"
    author = "QuantStack"
    url = "https://github.com/sparrow-org/sparrow-ipc"
    homepage = "https://github.com/sparrow-org/sparrow-ipc"
    topics = ("arrow", "apache arrow", "columnar format", "dataframe", "ipc", "serialization", "deserialization", "flatbuffers")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    exports_sources = "include/*", "src/*", "cmake/*", "docs/*", "CMakeLists.txt", "LICENSE"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "build_tests": [True, False],
        "generate_documentation": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "build_tests": False,
        "generate_documentation": False,
    }

    _flatbuffers_version = "24.12.23"

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def requirements(self):
        self.requires("sparrow/2.0.0")
        self.requires(f"flatbuffers/{self._flatbuffers_version}")
        self.requires("lz4/1.9.4")
        self.requires("zstd/1.5.7")
        if self.options.get_safe("build_tests"):
            self.test_requires("doctest/2.4.12")

    def build_requirements(self):
        self.tool_requires("cmake/[>=3.28.1 <4.2.0]")
        if self.options.get_safe("generate_documentation"):
            self.tool_requires("doxygen/[>=1.9.4 <2.0.0]", options={"enable_app": "True"})
        self.tool_requires(f"flatbuffers/{self._flatbuffers_version}")

    @property
    def _min_cppstd(self):
        return 20

    @property
    def _compilers_minimum_version(self):
        return {
            "apple-clang": "16",
            "clang": "18",
            "gcc": "12",
            "msvc": "194"
        }

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd)

        minimum_version = self._compilers_minimum_version.get(
            str(self.settings.compiler), False)
        if minimum_version and Version(self.settings.compiler.version) < minimum_version:
            raise ConanInvalidConfiguration(
                f"{self.ref} requires C++{self._min_cppstd}, which your compiler does not support."
            )

    def layout(self):
        cmake_layout(self, src_folder=".")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.variables["SPARROW_IPC_BUILD_SHARED"] = self.options.shared
        tc.variables["SPARROW_IPC_BUILD_TESTS"] = self.options.build_tests
        tc.variables["SPARROW_IPC_BUILD_DOCS"] = self.options.generate_documentation
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE",
             dst=os.path.join(self.package_folder, "licenses"),
             src=self.source_folder)
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["sparrow-ipc"]
        self.cpp_info.set_property("cmake_file_name", "sparrow-ipc")
        self.cpp_info.set_property("cmake_target_name", "sparrow-ipc::sparrow-ipc")
