package ru.otus.homework.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object HdfsService {

  def listDirs(root: Path)(implicit fs: FileSystem): Array[Path] =
    fs.listStatus(root).filter(_.isDirectory).map(_.getPath)

  def getFirstFileName(root: Path)(implicit fs: FileSystem): Option[Path] = {
    val files = fs.listStatus(root).filterNot(_.isDirectory).map(_.getPath)
    if (files.nonEmpty) Some(files.minBy(_.getName)) else None
  }

  def copyMergeFiles(srcDir: Path, dstDirName: String, conf: Configuration)(implicit fs: FileSystem): Boolean = {
    //предполагаю, что файлы Mac OS - случайные
    val files = fs.listStatus(srcDir).filter(_.isFile).map(_.getPath).filterNot(_.getName == ".DS_Store")
    val targetFileName = if (files.nonEmpty) Some(files.minBy(_.getName)) else None
    val targetDirName = s"$dstDirName/${srcDir.getName}"

    if (targetFileName.isDefined) {
      val out = fs.create(new Path(s"$targetDirName/${targetFileName.get.getName}"))
      try {
        files.foreach(file => {
          val in = fs.open(file)
          // закрываем стримы руками, тк пишем все в один out
          println("writing file")
          try {
            IOUtils.copyBytes(in, out, conf, false)
          } finally {
            in.close()
          }
        })
      } finally {
        out.close()
      }
    }

    fs.delete(srcDir, true)
  }

  def move(root: Path, target: String, conf: Configuration)(implicit fs: FileSystem): Unit = {
    listDirs(root).foreach(copyMergeFiles(_, target, conf))
  }
}
