%module GACD_interface


%apply double *OUTPUT {double *Q}


%{
#define SWIG_FILE_WITH_INIT
#include "GACD_interface.h"
#include "rt_nonfinite.h"
#include "GACD_emxAPI.h"
#include "eml_rand_mt19937ar_stateful.h"
#include "GACD_emxutil.h"
#include "compute_Q2.h"
#include "convert_labels.h"
#include "diff.h"
#include "find_communities.h"
#include "GACD_data.h"
#include "GACD_initialize.h"
#include "GACD_terminate.h"
#include "GACD_types.h"
#include "heapsort.h"
#include "insertionsort.h"
#include "introsort.h"
#include "Mutation.h"
#include "rand.h"
#include "rtGetInf.h"
#include "rtGetNaN.h"
#include "rtwtypes.h"
#include "sort1.h"
#include "sortIdx.h"
#include "sparse.h"
#include "sparse1.h"
#include "sum.h"
%}

%include "numpy.i"

%init %{
    import_array();
%}

%apply (double* IN_ARRAY1, int DIM1) {(double* b_W1, int size_w)}
%apply (double** ARGOUTVIEW_ARRAY1, int* DIM1) {(double** label, int* size_L), (double** x_1, int* size_x), (double** y_1, int* size_y)};



%include "GACD_interface.h"
