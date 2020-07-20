/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * main.cpp
 *
 * Code generation for function 'main'
 *
 */

/*************************************************************************/
/* This automatically generated example C main file shows how to call    */
/* entry-point functions that MATLAB Coder generated. You must customize */
/* this file for your application. Do not modify this file directly.     */
/* Instead, make a copy of this file, modify it, and integrate it into   */
/* your development environment.                                         */
/*                                                                       */
/* This file initializes entry-point function arguments to a default     */
/* size and value before calling the entry-point functions. It does      */
/* not store or use any values returned from the entry-point functions.  */
/* If necessary, it does pre-allocate memory for returned values.        */
/* You can use this file as a starting point for a main function that    */
/* you can deploy in your application.                                   */
/*                                                                       */
/* After you copy the file, and before you deploy it, you must make the  */
/* following changes:                                                    */
/* * For variable-size function arguments, change the example sizes to   */
/* the sizes that your application requires.                             */
/* * Change the example values of function arguments to the values that  */
/* your application requires.                                            */
/* * If the entry-point functions return values, store these values or   */
/* otherwise use them as required by your application.                   */
/*                                                                       */
/*************************************************************************/
/* Include files */
#include "rt_nonfinite.h"
//#include "GACD.h"
#include "main.h"
#include "GACD_terminate.h"
#include "GACD_emxAPI.h"
#include "GACD_initialize.h"
#include "eml_rand_mt19937ar_stateful.h"
#include "GACD_emxutil.h"
#include <iostream>
#include <time.h>
#include <fstream>
#include <vector>
#include <string>  
#include <sstream>  
#include <stdio.h>
#include "GACD_interface.h"
using namespace std;
/* Function Declarations */

static double b_W[3678] = { 2, 5, 10, 17, 24, 34, 36, 42, 66, 91,
  94, 105, 1, 26, 28, 34, 38, 46, 58, 90, 102, 104, 106, 110, 4, 7, 14, 15, 16,
  48, 61, 65, 73, 75, 101, 107, 3, 6, 12, 27, 41, 53, 59, 73, 75, 82, 85, 103,
  1, 6, 10, 17, 24, 29, 42, 70, 94, 105, 109, 4, 5, 11, 12, 53, 75, 82, 85, 91,
  98, 99, 108, 3, 8, 33, 40, 48, 56, 59, 61, 65, 86, 101, 107, 7, 9, 22, 23,
  41, 69, 74, 78, 79, 83, 109, 112, 8, 10, 22, 23, 42, 52, 69, 78, 79, 91, 112,
  1, 5, 9, 17, 23, 24, 42, 65, 94, 105, 109, 6, 12, 61, 73, 75, 82, 85, 99,
  103, 108, 4, 6, 11, 25, 29, 51, 70, 91, 98, 105, 14, 15, 18, 19, 27, 35, 37,
  39, 44, 86, 3, 13, 16, 33, 40, 46, 61, 65, 101, 107, 111, 3, 13, 16, 27, 39,
  44, 55, 72, 86, 100, 3, 14, 15, 33, 40, 48, 61, 69, 93, 101, 107, 115, 1, 5,
  10, 18, 24, 39, 42, 68, 82, 94, 105, 13, 17, 21, 28, 59, 63, 66, 88, 96, 97,
  114, 13, 20, 32, 35, 37, 39, 43, 55, 62, 72, 100, 19, 30, 31, 34, 36, 37, 45,
  56, 80, 95, 102, 18, 22, 37, 63, 66, 71, 76, 77, 88, 97, 114, 8, 9, 21, 23,
  33, 47, 52, 69, 78, 109, 112, 8, 9, 10, 22, 24, 48, 52, 69, 78, 79, 109, 1,
  5, 10, 17, 23, 42, 79, 91, 94, 105, 112, 12, 26, 29, 51, 67, 70, 85, 88, 91,
  111, 2, 25, 34, 38, 46, 54, 90, 104, 106, 107, 110, 4, 13, 15, 28, 35, 39,
  43, 44, 62, 86, 2, 18, 27, 57, 63, 64, 66, 71, 77, 96, 97, 5, 12, 25, 39, 51,
  70, 79, 91, 114, 20, 31, 36, 43, 56, 80, 81, 83, 92, 95, 102, 20, 30, 36, 45,
  51, 56, 80, 83, 95, 102, 110, 19, 33, 35, 44, 55, 56, 62, 72, 80, 86, 100, 7,
  14, 16, 22, 32, 40, 48, 50, 65, 101, 107, 1, 2, 20, 26, 38, 46, 90, 104, 106,
  110, 13, 19, 27, 32, 36, 43, 55, 62, 72, 95, 100, 1, 20, 30, 31, 35, 45, 56,
  80, 93, 95, 102, 13, 19, 20, 21, 38, 44, 59, 60, 2, 26, 34, 37, 46, 81, 90,
  96, 104, 106, 110, 13, 15, 17, 19, 27, 29, 40, 44, 55, 72, 86, 7, 14, 16, 33,
  39, 48, 55, 61, 83, 101, 107, 4, 8, 42, 52, 53, 73, 75, 82, 99, 103, 108, 1,
  5, 9, 10, 17, 24, 41, 68, 94, 105, 19, 27, 30, 35, 44, 58, 64, 13, 15, 27,
  32, 37, 39, 43, 62, 71, 80, 86, 20, 31, 36, 46, 49, 58, 67, 76, 87, 92, 113,
  2, 14, 26, 34, 38, 45, 63, 90, 104, 106, 110, 22, 48, 50, 54, 68, 74, 84, 89,
  111, 112, 115, 3, 7, 16, 23, 33, 40, 47, 61, 62, 65, 101, 45, 50, 54, 58, 67,
  76, 87, 92, 93, 97, 99, 33, 47, 49, 54, 68, 74, 84, 85, 89, 111, 115, 12, 25,
  29, 31, 52, 69, 70, 79, 91, 9, 22, 23, 41, 51, 69, 78, 79, 102, 109, 112, 4,
  6, 41, 54, 73, 75, 85, 99, 103, 113, 26, 47, 49, 50, 53, 68, 74, 84, 87, 89,
  111, 115, 15, 19, 32, 35, 39, 40, 56, 62, 72, 100, 7, 20, 30, 31, 32, 36, 55,
  80, 90, 95, 102, 28, 58, 63, 66, 71, 77, 88, 96, 97, 107, 2, 43, 45, 49, 57,
  76, 87, 92, 93, 113, 4, 7, 18, 37, 60, 64, 89, 98, 102, 115, 37, 59, 61, 64,
  67, 77, 98, 114, 3, 7, 11, 14, 16, 40, 48, 60, 65, 72, 107, 19, 27, 32, 35,
  44, 48, 55, 63, 72, 93, 100, 18, 21, 28, 46, 57, 62, 71, 77, 88, 96, 106, 28,
  43, 59, 60, 65, 66, 98, 110, 113, 3, 7, 10, 14, 33, 48, 61, 64, 101, 107,
  112, 1, 18, 21, 28, 57, 64, 67, 71, 88, 97, 114, 25, 45, 49, 60, 66, 76, 77,
  87, 92, 93, 113, 17, 42, 47, 50, 54, 69, 74, 84, 89, 105, 111, 115, 8, 9, 16,
  22, 23, 51, 52, 68, 79, 109, 112, 5, 12, 25, 29, 51, 71, 84, 89, 91, 92, 96,
  21, 28, 44, 57, 63, 66, 70, 77, 96, 104, 114, 15, 19, 32, 35, 39, 55, 61, 62,
  73, 100, 3, 4, 11, 41, 53, 72, 75, 82, 103, 105, 108, 8, 47, 50, 54, 68, 75,
  78, 84, 89, 111, 115, 3, 4, 6, 11, 41, 53, 73, 74, 83, 85, 103, 21, 45, 49,
  58, 67, 77, 87, 93, 108, 113, 21, 28, 57, 60, 63, 67, 71, 76, 96, 97, 114, 8,
  9, 22, 23, 52, 74, 79, 83, 99, 109, 112, 8, 9, 23, 24, 29, 51, 52, 69, 78,
  109, 112, 20, 30, 31, 32, 36, 44, 56, 81, 95, 102, 110, 30, 38, 80, 83, 86,
  87, 92, 94, 95, 106, 111, 4, 6, 11, 17, 41, 73, 83, 84, 85, 99, 108, 8, 30,
  31, 40, 75, 78, 81, 82, 94, 95, 101, 47, 50, 54, 68, 70, 74, 82, 85, 89, 111,
  115, 4, 6, 11, 25, 50, 53, 75, 82, 84, 99, 108, 7, 13, 15, 27, 32, 39, 44,
  81, 100, 45, 49, 54, 58, 67, 76, 81, 88, 92, 93, 98, 18, 21, 25, 57, 63, 66,
  87, 96, 97, 105, 114, 47, 50, 54, 59, 68, 70, 74, 84, 90, 108, 111, 115, 2,
  26, 34, 38, 46, 56, 89, 100, 104, 106, 110, 1, 6, 9, 12, 24, 25, 29, 51, 70,
  30, 45, 49, 58, 67, 70, 81, 87, 93, 94, 113, 16, 36, 49, 58, 62, 67, 76, 87,
  92, 107, 113, 1, 5, 10, 17, 24, 42, 81, 83, 92, 105, 20, 30, 31, 35, 36, 56,
  80, 81, 83, 102, 18, 28, 38, 57, 63, 70, 71, 77, 88, 114, 18, 21, 28, 49, 57,
  66, 77, 88, 113, 114, 6, 12, 59, 60, 64, 87, 99, 113, 6, 11, 41, 49, 53, 78,
  82, 85, 98, 103, 108, 15, 19, 32, 35, 55, 62, 72, 86, 90, 101, 3, 7, 14, 16,
  33, 40, 48, 65, 83, 100, 103, 2, 20, 30, 31, 36, 52, 56, 59, 80, 95, 4, 11,
  41, 53, 73, 75, 99, 101, 104, 108, 2, 26, 34, 38, 46, 71, 90, 103, 106, 110,
  1, 5, 10, 12, 17, 24, 42, 68, 73, 88, 94, 115, 2, 26, 34, 38, 46, 63, 81, 90,
  104, 110, 3, 7, 14, 16, 26, 33, 40, 57, 61, 65, 93, 6, 11, 41, 73, 76, 82,
  85, 89, 99, 103, 5, 8, 10, 22, 23, 52, 69, 78, 79, 112, 2, 26, 31, 34, 38,
  46, 64, 80, 90, 104, 106, 14, 25, 47, 50, 54, 68, 74, 81, 84, 89, 115, 8, 9,
  22, 24, 47, 52, 65, 69, 78, 79, 109, 45, 53, 58, 64, 67, 76, 92, 93, 97, 98,
  18, 21, 29, 60, 66, 71, 77, 88, 96, 97, 16, 47, 50, 54, 59, 68, 74, 84, 89,
  105, 111, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
  2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
  5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9,
  9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11,
  11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13,
  13, 13, 13, 13, 13, 13, 13, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15,
  15, 15, 15, 15, 15, 15, 15, 15, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
  16, 16, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 18, 18, 18, 18, 18, 18,
  18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 20, 20, 20,
  20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21,
  22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 23, 23, 23,
  23, 23, 23, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25,
  25, 25, 25, 25, 25, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 27, 27, 27,
  27, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 29,
  29, 29, 29, 29, 29, 29, 29, 29, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30,
  31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 32, 32, 32, 32, 32, 32, 32, 32,
  32, 32, 32, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 34, 34, 34, 34, 34,
  34, 34, 34, 34, 34, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 36, 36, 36,
  36, 36, 36, 36, 36, 36, 36, 36, 37, 37, 37, 37, 37, 37, 37, 37, 38, 38, 38,
  38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
  40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 41, 41, 41, 41, 41, 41, 41, 41,
  41, 41, 41, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 43, 43, 43, 43, 43, 43,
  43, 44, 44, 44, 44, 44, 44, 44, 44, 44, 44, 44, 45, 45, 45, 45, 45, 45, 45,
  45, 45, 45, 45, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 47, 47, 47, 47,
  47, 47, 47, 47, 47, 47, 47, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49,
  49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 50, 50, 50, 50, 50, 50, 50, 50, 50,
  50, 50, 51, 51, 51, 51, 51, 51, 51, 51, 51, 52, 52, 52, 52, 52, 52, 52, 52,
  52, 52, 52, 53, 53, 53, 53, 53, 53, 53, 53, 53, 53, 54, 54, 54, 54, 54, 54,
  54, 54, 54, 54, 54, 54, 55, 55, 55, 55, 55, 55, 55, 55, 55, 55, 56, 56, 56,
  56, 56, 56, 56, 56, 56, 56, 56, 57, 57, 57, 57, 57, 57, 57, 57, 57, 57, 58,
  58, 58, 58, 58, 58, 58, 58, 58, 58, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59,
  60, 60, 60, 60, 60, 60, 60, 60, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61,
  62, 62, 62, 62, 62, 62, 62, 62, 62, 62, 62, 63, 63, 63, 63, 63, 63, 63, 63,
  63, 63, 63, 64, 64, 64, 64, 64, 64, 64, 64, 64, 65, 65, 65, 65, 65, 65, 65,
  65, 65, 65, 65, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 67, 67, 67, 67,
  67, 67, 67, 67, 67, 67, 67, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68,
  69, 69, 69, 69, 69, 69, 69, 69, 69, 69, 69, 70, 70, 70, 70, 70, 70, 70, 70,
  70, 70, 70, 71, 71, 71, 71, 71, 71, 71, 71, 71, 71, 71, 72, 72, 72, 72, 72,
  72, 72, 72, 72, 72, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 74, 74, 74,
  74, 74, 74, 74, 74, 74, 74, 74, 75, 75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
  76, 76, 76, 76, 76, 76, 76, 76, 76, 76, 77, 77, 77, 77, 77, 77, 77, 77, 77,
  77, 77, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 79, 79, 79, 79, 79, 79,
  79, 79, 79, 79, 79, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 81, 81, 81,
  81, 81, 81, 81, 81, 81, 81, 81, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82,
  83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 84, 84, 84, 84, 84, 84, 84, 84,
  84, 84, 84, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 86, 86, 86, 86, 86,
  86, 86, 86, 86, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 88, 88, 88, 88,
  88, 88, 88, 88, 88, 88, 88, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89,
  90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 91, 91, 91, 91, 91, 91, 91, 91,
  91, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 93, 93, 93, 93, 93, 93, 93,
  93, 93, 93, 93, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 95, 95, 95, 95, 95,
  95, 95, 95, 95, 95, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 97, 97, 97, 97,
  97, 97, 97, 97, 97, 97, 98, 98, 98, 98, 98, 98, 98, 98, 99, 99, 99, 99, 99,
  99, 99, 99, 99, 99, 99, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
  101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 102, 102, 102, 102,
  102, 102, 102, 102, 102, 102, 103, 103, 103, 103, 103, 103, 103, 103, 103,
  103, 104, 104, 104, 104, 104, 104, 104, 104, 104, 104, 105, 105, 105, 105,
  105, 105, 105, 105, 105, 105, 105, 105, 106, 106, 106, 106, 106, 106, 106,
  106, 106, 106, 107, 107, 107, 107, 107, 107, 107, 107, 107, 107, 107, 108,
  108, 108, 108, 108, 108, 108, 108, 108, 108, 109, 109, 109, 109, 109, 109,
  109, 109, 109, 109, 110, 110, 110, 110, 110, 110, 110, 110, 110, 110, 110,
  111, 111, 111, 111, 111, 111, 111, 111, 111, 111, 111, 112, 112, 112, 112,
  112, 112, 112, 112, 112, 112, 112, 113, 113, 113, 113, 113, 113, 113, 113,
  113, 113, 114, 114, 114, 114, 114, 114, 114, 114, 114, 114, 115, 115, 115,
  115, 115, 115, 115, 115, 115, 115, 115, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };

double pop_size = 80;
double L = 200;  double p1 = 0.8;
double **label;
double **x_1; double **y_1;
double Q;
int nval = 1226;
int dim_w;
int *dim_L;
int *dim_x;
int *dim_y;
emxArray_real_T *labels;
emxArray_real_T *x;
emxArray_real_T *y;

 
int main(int, const char * const[])
{
	/* Initialize the application.
	   You do not need to do this more than one time. */
	//GACD_initialize();

	/* Invoke the entry-point functions.
	   You can call entry-point functions multiple times. */
    GACD(b_W, dim_w, nval, pop_size, L, p1, Q, label, dim_L, x_1, dim_x, y_1, dim_y);

	/* Terminate the application.
	   You do not need to do this more than one time. */
	//GACD_terminate();
	return 0;
}

/* End of code generation (main.cpp) */


