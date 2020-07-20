from distutils.core import setup, Extension
import numpy



#生成一个扩展模块
pht_module = Extension('_GACD_interface', #模块名称，必须要有下划线
                        sources=['GACD_interface_wrap.cxx', #封装后的接口cxx文件
                                 'GACD_interface.cpp',
                                 'rt_nonfinite.cpp',
                                 #'GACD.cpp',
                                 #'GACD_terminate.cpp',
                                 'GACD_emxAPI.cpp',
                                 #'GACD_initialize.cpp',
                                 'eml_rand_mt19937ar_stateful.cpp',
                                 'GACD_emxutil.cpp',
                                 'compute_Q2.cpp',
                                 'convert_labels.cpp',
                                 'diff.cpp',
                                 'find_communities.cpp',
                                 'GACD_data.cpp',
                                 'GACD_initialize.cpp',
                                 'GACD_terminate.cpp',
                                 'heapsort.cpp',
                                 'insertionsort.cpp',
                                 'introsort.cpp',
                                 'Mutation.cpp',
                                 'rand.cpp',
                                 'rtGetInf.cpp',
                                 'rtGetNaN.cpp',
                                 'sort1.cpp',
                                 'sortIdx.cpp',
                                 'sparse.cpp',
                                 'sparse1.cpp',
                                 'sum.cpp'
                                ],
                            extra_compile_args=["-stdlib=libc++"],
                            extra_link_args=['-stdlib=libc++'],
                            include_dirs=[numpy.get_include()],
                      )

setup(name = 'GACD_interface',	#打包后的名称
        version = '0.1',
        author = 'SWIG Docs',
        description = 'Simple swig pht from docs',
        ext_modules = [pht_module], #与上面的扩展模块名称一致
        py_modules = ['GACD_interface'], #需要打包的模块列表
    )