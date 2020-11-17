#include <ccl.h>
#include <daal.h>

#include "service.h"

#include <chrono>
#include <iostream>

#include "org_apache_spark_ml_feature_PCADALImpl.h"

using namespace std;
using namespace daal;
using namespace daal::algorithms;

const int ccl_root = 0;

typedef double algorithmFPType; /* Algorithm floating-point type */

/*
 * Class:     org_apache_spark_ml_feature_PCADALImpl
 * Method:    cPCATrainDAL
 * Signature: (JIIILorg/apache/spark/ml/feature/PCAResult;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_spark_ml_feature_PCADALImpl_cPCATrainDAL(
    JNIEnv *env, jobject obj, jlong pNumTabData, jint k, jint executor_num, jint executor_cores,
    jobject resultObj) {
  size_t rankId;
  ccl_get_comm_rank(NULL, &rankId);

  const size_t nBlocks = executor_num;
  const int comm_size = executor_num;

  NumericTablePtr pData = *((NumericTablePtr*)pNumTabData);
  // Source data already normalized
  pData->setNormalizationFlag(NumericTableIface::standardScoreNormalized);

  // Set number of threads for oneDAL to use for each rank
  services::Environment::getInstance()->setNumberOfThreads(executor_cores);

  int nThreadsNew = services::Environment::getInstance()->getNumberOfThreads();
  cout << "oneDAL (native): Number of threads used: " << nThreadsNew << endl;

  pca::Distributed<step1Local, algorithmFPType, pca::svdDense> localAlgorithm;

  /* Set the input data set to the algorithm */
  localAlgorithm.input.set(pca::data, pData);

  /* Compute PCA decomposition */
  localAlgorithm.compute();

  /* Serialize partial results required by step 2 */
  services::SharedPtr<byte> serializedData;
  InputDataArchive dataArch;
  localAlgorithm.getPartialResult()->serialize(dataArch);
  size_t perNodeArchLength = dataArch.getSizeOfArchive();

  serializedData = services::SharedPtr<byte>(new byte[perNodeArchLength * nBlocks]);

  byte* nodeResults = new byte[perNodeArchLength];
  dataArch.copyArchiveToArray(nodeResults, perNodeArchLength);

  ccl_request_t request;  

  size_t* recv_counts = new size_t[comm_size * perNodeArchLength];
  for (int i = 0; i < comm_size; i++) recv_counts[i] = perNodeArchLength;

  cout << "PCA (native): ccl_allgatherv receiving " << perNodeArchLength * nBlocks << " bytes" << endl;

  auto t1 = std::chrono::high_resolution_clock::now();

  /* Transfer partial results to step 2 on the root node */
  // MPI_Gather(nodeResults, perNodeArchLength, MPI_CHAR, serializedData.get(),
  // perNodeArchLength, MPI_CHAR, ccl_root, MPI_COMM_WORLD);
  ccl_allgatherv(nodeResults, perNodeArchLength, serializedData.get(), recv_counts,
                 ccl_dtype_char, NULL, NULL, NULL, &request);
  ccl_wait(request);

  auto t2 = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::seconds>( t2 - t1 ).count();
  std::cout << "PCA (native): ccl_allgatherv took " << duration << " secs" << std::endl;

  delete[] nodeResults;

  if (rankId == ccl_root) {
    auto t1 = std::chrono::high_resolution_clock::now();        

    /* Create an algorithm for principal component analysis using the svdDense method
     * on the master node */
    pca::Distributed<step2Master, algorithmFPType, pca::svdDense> masterAlgorithm;

    for (size_t i = 0; i < nBlocks; i++) {
      /* Deserialize partial results from step 1 */
      OutputDataArchive dataArch(serializedData.get() + perNodeArchLength * i,
                                 perNodeArchLength);

      services::SharedPtr<pca::PartialResult<pca::svdDense> >
          dataForStep2FromStep1 =
              services::SharedPtr<pca::PartialResult<pca::svdDense> >(
                  new pca::PartialResult<pca::svdDense>());
      dataForStep2FromStep1->deserialize(dataArch);

      /* Set local partial results as input for the master-node algorithm */
      masterAlgorithm.input.add(pca::partialResults, dataForStep2FromStep1);
    }

    /* Merge and finalizeCompute PCA decomposition on the master node */
    masterAlgorithm.compute();
    masterAlgorithm.finalizeCompute();

    /* Retrieve the algorithm results */
    pca::ResultPtr result = masterAlgorithm.getResult();

    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>( t2 - t1 ).count();
    std::cout << "PCA (native): master step took " << duration << " secs" << std::endl;

    /* Print the results */
    printNumericTable(result->get(pca::eigenvalues), "First 10 Eigenvalues:", 10);
    printNumericTable(result->get(pca::eigenvectors), "First 10 Eigenvectors:", 10);

    // Return all eigenvalues & eigenvectors

    // Get the class of the input object
    jclass clazz = env->GetObjectClass(resultObj);
    // Get Field references
    jfieldID pcNumericTableField = env->GetFieldID(clazz, "pcNumericTable", "J");
    jfieldID explainedVarianceNumericTableField = env->GetFieldID(clazz, "explainedVarianceNumericTable", "J");

    NumericTablePtr *eigenvalues = new NumericTablePtr(result->get(pca::eigenvalues));
    NumericTablePtr *eigenvectors = new NumericTablePtr(result->get(pca::eigenvectors));

    env->SetLongField(resultObj, pcNumericTableField, (jlong)eigenvectors);
    env->SetLongField(resultObj, explainedVarianceNumericTableField, (jlong)eigenvalues);
  }

  return 0;
}
