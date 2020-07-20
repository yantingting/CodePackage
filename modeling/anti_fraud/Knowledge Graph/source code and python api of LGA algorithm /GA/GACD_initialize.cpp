/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * GACD_initialize.cpp
 *
 * Code generation for function 'GACD_initialize'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "GACD_initialize.h"
#include "eml_rand_mt19937ar_stateful.h"

/* Function Definitions */
void GACD_initialize()
{
  rt_InitInfAndNaN(8U);
  c_eml_rand_mt19937ar_stateful_i();
}

/* End of code generation (GACD_initialize.cpp) */
