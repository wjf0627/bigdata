package com.jinfeng.spark.example.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2020/7/13
 * @time: 3:51 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object SparkListExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("ThreadSpark")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .getOrCreate()
    val sc = spark.sparkContext

    //  SparkUtil.listDirs("/")

    val output = "/Users/wangjf/data/2021/01/20"
    println("path.exists-->>" + FileSystem.get(sc.hadoopConfiguration).exists(new Path(output)))

    import spark.implicits._

    val df = sc.parallelize(List("8f5ca319fe39d856",
    "abfdb7de-a9e7-344d-beaf-e9fff7aa5d58",
    "ffcbae80-9a08-4667-ad0f-320180d34951",
    "890914da857cf0c2",
    "fa3fbdbb-7a57-863c-959d-53b5ff7b6d9d",
    "b96ffebd-bf7b-168d-ffef-add1f7fc8ae1",
    "ff79bfe7-37ff-2080-f69a-6ef4b73f5690",
    "7499419452bd3961",
    "bed76b6d-90f7-2018-3fb3-e7f7bbe76f20",
    "bff3e3ff-569b-ed1b-ffff-b66f9dd5acbb",
    "f7bf57fc-f9cd-95a8-f37f-fbffff7dfe1b",
    "d2d992e79d9b7e3b",
    "348c75dc16c7dfe5",
    "f6fefe9f-fdaf-0650-bd3f-ff7ff7f7ba06",
    "db7d1d99-d202-4c66-b27f-f1bdda7f3bf5",
    "8132d7ae3019e87fee9d4189e18e949d86b6c876e385848d87d82aceb9d2f5c1",
    "d1a9b405-ccdf-479f-92ad-379f43936798",
    "f5eddfdd-8d6f-394e-bf8f-c9f5bf392f91",
    "520aec4206fc9832",
    "d460d1bd0e82964",
    "beef93e4d060e0c",
    "8d017533-a79e-45f2-85d3-ef9abe66fb49",
    "fe5fffbd-fd17-fb93-dfff-eefdfef68256",
    "de6b9686ee0189ab",
    "f7ddfe86-fd7e-a016-f7fb-fbe7ef9e9e8d",
    "a70d7c993a217cbeea42c0ca2b3cb3b437024b8904404333d74ff411534fda78",
    "683d14f7-cdb3-429c-9d28-46cfae5a590e",
    "1d67a9f138d2a20a",
    "cfa1b8ee-eb72-4b05-b668-0f35c169fc28",
    "144c2826c654d6fe",
    "e2315b9c2b7adbfc",
    "ccf80aec99b21451f169a499eba3c1b3841ac767312aa765f65a992bca28e527",
    "7fffbe9b-c8f7-1a69-eedb-7adbf8570b25",
    "fd74bbf5-f739-2dc6-7f6f-3dbfff7d2603",
    "3cf2efed-fffb-9a94-f79e-fd8fbffea077",
    "951ea9f2ff0d2d29",
    "ff7bf7fa-b7df-5420-ffde-f3f97dff5f1b",
    "cd4a4752d7948f75",
    "05e9a4af-de7e-4039-adef-ea0e2b59322c",
    "4f3db65badd519db",
    "87b50043717ab06a",
    "da381b1d-4240-4e5f-aabe-2d7b1bf5ae79",
    "97c1f7d7-7ee2-4554-b9be-ef50bed5c463",
    "ff394e73bb581e5a",
    "b5ecfbef-b7bf-7b6c-3f3f-7bb5f3df0847",
    "7a94cecd-dfef-512e-ffe9-367fd77f90a9",
    "23b18ab8ba7cc84e",
    "810e88a5a99d1285",
    "b970fd7f-dea7-0f21-fdeb-2ed9fdbdc086",
    "3d964183-5e97-4f1f-bb82-61fd7a28d85a",
    "2bd859df-a5ef-82a6-5b77-8f5fedfe006a",
    "1c187428eae39382",
    "e6e6af87-ff4e-ee41-e2ce-ffff77dd2b0c",
    "ffe5bffc-fff7-5c88-e33f-dff367fa6200",
    "eb6f54c1e22789f3",
    "3dcf9f7c-fef3-5026-de3a-7febdfbfae6b",
    "90dcfdd16ee7f112",
    "14942b23b810bd9aace1c42b6f2515d9c47319b4facda70db7cff83bc2c78faa",
    "77e5f7e3-ffff-2309-fbd7-5fbff3eea654",
    "10882d5460544f9e",
    "773c3f8ca3e631b5463e4fa1c51ee05541e50684df65dc07e3affabcd03db3ba",
    "c68b66d3-8941-4b9c-9a73-f6c23839442d",
    "406f305cbe6ce68e",
    "efcefbff-dd7e-463f-faf5-bffdeffbeca4",
    "570448dff01cad20",
    "af790de4c2147d8c",
    "a9f7fffa-a3fd-0ab1-7e2a-fefbf9f7f9b6",
    "06c4f09b56800db902857e93fb47dc44b497e57d018a4ce87d0bbc9dfde12f46",
    "92cb5657-0d8a-42d5-934d-32a56147421f",
    "cebde19f5639b53f9c9a8ae8b60111c17b592830a5e38cef6611cd198a26af0e",
    "5455e727d8b124ac1a709b7f604c6489ee9b80c5c1225ddc898980abaa627296",
    "558f72d889a2615a",
    "4591f514-1b2d-4259-86ab-14ba6238a353",
    "3afec2a2c4337e99",
    "0942fd84-6858-42e3-83d0-09677edbe633",
    "8c3f06614918977d",
    "2efe78ef-fdef-5ae3-dc79-f7df6e1dff89",
    "b074c6e3-16f8-4b96-8932-299072de6c66",
    "445cf55535d565f6a0dc017a5d5167cd44a7f38b3dacd379b6e552003d2baec4",
    "8f407b181b4f2df3",
    "3e91cfb2d96467ee65f249cd9998c21b68683d2871cf873764ab933e02de85d0",
    "ae7f7eef-7f57-d5c6-6df7-ffaff6fe08e2",
    "1b6c08807a79c317",
    "d03c4cbff0ce5629ec87dd90ac2feec12209e1e518d99dbfca1cf0320820102c",
    "7b3a5f7e-dbe8-54d9-fdff-74fedfb7710e",
    "ffbca7ea-4eb1-21e1-fef1-5efe7dedecbe",
    "70dd5a978fedda580712dbfb7edae1922a0618e846568e98f0d38fa31b7a4c05",
    "d3b66b5db5d4e577",
    "c6c7f93c-d778-4710-809a-842371339b1e",
    "fd92efd74d33c2b3",
    "3b085f79-f336-444d-a040-af55a767907a",
    "b58b529776baeee7",
    "8db75fb5ae66e5f7",
    "2533cb57e0b233e6",
    "866838045394240",
    "863667043619313",
    "863713035247331",
    "A00000810156CE",
    "869829043051657",
    "868227030244149",
    "768525431994829",
    "867673043719679",
    "866369036800839",
    "867394036194925",
    "869775033713204",
    "865137035279779",
    "864351044780957",
    "867279021387644",
    "866621034393690",
    "869096036745964",
    "867134030379936",
    "A1000050156308",
    "99001238930321",
    "862517057919219",
    "867303038742615",
    "A0000092742B23",
    "861406035777975",
    "866399032906405",
    "869114036351608",
    "869014040134802",
    "860440032954957",
    "860406043055811",
    "862859031773134",
    "867548043229228",
    "865126047785792",
    "868082045850236",
    "863999031947798",
    "868947031354288",
    "866681048772735",
    "863873051696835",
    "A1000065505F3A",
    "359596063057123",
    "863873059770996",
    "864714047264198",
    "99001140075646",
    "860823047046465",
    "869497031380239",
    "355637948006349",
    "865803040440837",
    "86795103668606",
    "868480032273240",
    "869458043825512",
    "865680046560630")).toDF("device_id")

    df.createOrReplaceTempView("tmp_dev")
    val install_df = spark.sql("SELECT * FROM dwh JOIN tmp dev ON device_id")
    val rdd = sc.parallelize(List((1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1)))
      .combineByKey(
        (v: Int) => v,
        (c: Int, v: Int) => c + v,
        (c1: Int, c2: Int) => c1 + c2
      )

    val dff = rdd.toDF("key", "value")
    dff.createOrReplaceTempView("")
    spark.sql("").except(spark.sql(""))

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    dff.select("key").where("value > 5").write.orc(output)

    sc.stop()
  }
}

